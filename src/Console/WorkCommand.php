<?php

namespace Laraowl\Client\Console;

use Carbon\CarbonImmutable;
use Carbon\CarbonInterval;
use Illuminate\Console\Command;
use Illuminate\Support\Str;
use Laraowl\Client\HttpIngest;
use Laraowl\Client\RedisIngest;
use Laraowl\Client\Support\RedisAdapter;
use Throwable;

use function count;
use function function_exists;
use function is_array;
use function is_int;
use function is_string;
use function max;
use function pcntl_async_signals;
use function pcntl_signal;
use function serialize;
use function sprintf;
use function time;
use function unserialize;
use function usleep;

/**
 * Drain the LaraOwl Redis stream and forward batched records to the LaraOwl
 * server via HttpIngest. Designed to run as a long-lived sidecar:
 *
 *   php artisan laraowl:work --sleep=1 --chunk=200 --max-time=3600
 *
 * Behavior mirrors `pulse:work` so operators familiar with Pulse find the same
 * shape: drain in chunks, ack via XDEL on success, trim periodically, exit
 * cleanly on SIGTERM / --max-time.
 */
class WorkCommand extends Command
{
    /** @var string */
    protected $signature = 'laraowl:work
        {--sleep=1 : Seconds to sleep when the stream is empty.}
        {--chunk=200 : Maximum entries drained per XRANGE call.}
        {--max-time=0 : Stop after this many seconds (0 = forever).}
        {--once : Drain whatever is currently buffered and exit.}
        {--trim-every=60 : Run XTRIM at most every N seconds.}
        {--lock-ttl=60 : Single-worker lock TTL in seconds. Auto-renewed mid-drain.}
        {--no-single-worker : Skip the cluster-wide single-worker lock. Use with care.}';

    /** @var string */
    protected $description = 'Drain the LaraOwl Redis stream and forward batches to the server.';

    private bool $shouldStop = false;

    public function handle(RedisIngest $ingest, HttpIngest $http): int
    {
        $this->registerSignalHandlers();

        $start = CarbonImmutable::now();
        /** @var int $maxTime */
        $maxTime = (int) $this->option('max-time');
        /** @var int $chunk */
        $chunk = max(1, (int) $this->option('chunk'));
        /** @var float $sleep */
        $sleep = max(0.0, (float) $this->option('sleep'));
        /** @var int $trimEvery */
        $trimEvery = max(0, (int) $this->option('trim-every'));
        $once = (bool) $this->option('once');
        $skipLock = (bool) $this->option('no-single-worker');
        $lockTtl = max(5, (int) $this->option('lock-ttl'));
        $stream = $ingest->stream();
        $lastTrim = 0;
        $adapter = $ingest->adapter();

        // Cluster-wide single-worker guard. XRANGE/XDEL is not safe across
        // multiple drainers because two workers can read the same entries
        // before either deletes them — leading to duplicated transmit().
        //
        // The lock is implemented directly on top of the SAME Redis
        // connection that holds the stream (SET key NX EX), instead of
        // Laravel's Cache::lock — that way the lock and the stream share
        // a single source of truth even when the host application's cache
        // driver is `array` or some non-shared store, and the lock can
        // never claim safety it cannot deliver.
        //
        // The TTL is finite (default 60s, renewed mid-drain) so a worker
        // that crashes mid-cycle does not leave the stream undrainable.
        //
        // Pass --no-single-worker to bypass when an external consumer-group
        // scheme is in place.
        $lockKey = $stream.':lock';
        $lockValue = (string) Str::uuid();
        $lockHeld = false;
        $lastRenew = time();

        if (! $skipLock) {
            try {
                $lockHeld = $adapter->lockAcquire($lockKey, $lockValue, $lockTtl);
            } catch (Throwable $e) {
                $this->warn('laraowl:work lockAcquire failed ('.$e->getMessage().'); proceeding without single-worker guard.');
                $lockHeld = false;
            }

            if (! $lockHeld) {
                $lockHeld = $this->isLockRecoverable($adapter, $lockKey, $lockValue, $lockTtl);

                if (! $lockHeld) {
                    $this->info('laraowl:work another drainer holds the lock for stream ['.$stream.']; exiting.');

                    return self::SUCCESS;
                }
            }
        }

        try {
            while (! $this->shouldStop) {
                $entries = $this->drainOnce($adapter, $stream, $chunk, $http);

                if ($entries === 0) {
                    if ($once) {
                        return self::SUCCESS;
                    }
                    $this->maybeTrim($adapter, $stream, $ingest->trimKeep(), $lastTrim, $trimEvery);
                    $this->sleepIfNeeded($sleep);
                }

                // Renew the lock at roughly half its TTL so a long-running
                // drain does not lose it.
                if ($lockHeld && (time() - $lastRenew) >= max(1, (int) ($lockTtl / 2))) {
                    try {
                        if (! $adapter->lockRenew($lockKey, $lockValue, $lockTtl)) {
                            $this->warn('laraowl:work lock no longer owned by us; another drainer may have taken over. Exiting.');

                            return self::SUCCESS;
                        }
                    } catch (Throwable $e) {
                        $this->warn('laraowl:work lockRenew failed: '.$e->getMessage());
                    }
                    $lastRenew = time();
                }

                if ($maxTime > 0 && CarbonImmutable::now()->diffInSeconds($start) >= $maxTime) {
                    $this->info(sprintf('laraowl:work hit --max-time=%ds, exiting.', $maxTime));

                    return self::SUCCESS;
                }
            }

            $this->info('laraowl:work received shutdown signal, exiting.');

            return self::SUCCESS;
        } finally {
            if ($lockHeld) {
                try {
                    $adapter->lockRelease($lockKey, $lockValue);
                } catch (Throwable) {
                    // Lock release is best-effort; the finite TTL is the
                    // ultimate stale-recovery mechanism.
                }
            }
        }
    }

    /**
     * The lock may exist but be in the process of natural expiry — try once
     * to take ownership if it does not look like an active drainer's. This
     * prevents an indefinite stall after a worker crashed before the TTL
     * was set.
     */
    protected function isLockRecoverable(RedisAdapter $adapter, string $key, string $value, int $ttl): bool
    {
        try {
            return $adapter->lockAcquire($key, $value, $ttl);
        } catch (Throwable) {
            return false;
        }
    }

    /**
     * Drain at most $chunk entries from the stream. Returns the number of
     * entries actually processed. Acks (XDEL) only when the upstream HTTP
     * POST is confirmed delivered; failed transmits leave entries on the
     * stream for the next cycle to retry, so a slow/down LaraOwl server
     * never causes silent data loss.
     */
    protected function drainOnce(RedisAdapter $adapter, string $stream, int $chunk, HttpIngest $http): int
    {
        try {
            $entries = $adapter->xrange($stream, '-', '+', $chunk);
        } catch (Throwable $e) {
            $this->error('laraowl:work XRANGE failed: '.$e->getMessage());

            return 0;
        }

        if ($entries === []) {
            return 0;
        }

        $records = [];
        $ids = [];

        foreach ($entries as $id => $fields) {
            $ids[] = $id;
            $payload = $fields['data'] ?? null;
            if (! is_string($payload)) {
                continue;
            }

            $record = @unserialize($payload, ['allowed_classes' => false]);
            if ($record === false && $payload !== serialize(false)) {
                continue;
            }
            if (! is_array($record)) {
                continue;
            }
            $records[] = $record;
        }

        if ($records !== []) {
            $sent = false;
            try {
                $sent = $http->transmitWithResult($records);
            } catch (Throwable $e) {
                // HttpIngest already swallows network/Guzzle exceptions and
                // returns false; this is a defense-in-depth guard in case a
                // future refactor lets one escape.
                $this->error('laraowl:work transmit threw: '.$e->getMessage());
            }

            if (! $sent) {
                // Leave the stream intact so the next cycle retries; the
                // upstream server may have been overloaded, restarting, or
                // simply unreachable. Without this guard, a non-2xx response
                // would cause silent data loss because XDEL below would run
                // anyway.
                $this->warn('laraowl:work transmit reported failure; leaving '.count($ids).' entries on stream for retry.');

                return 0;
            }
        }

        try {
            $adapter->xdel($stream, $ids);
        } catch (Throwable $e) {
            // Records were already transmitted, so a duplicate transmit on
            // the next cycle is the only fallout — the server's `batch_id`
            // re-query is scoped per insert, so duplicates would land as
            // separate rows. Log so operators see this.
            $this->warn('laraowl:work XDEL failed (records were transmitted): '.$e->getMessage());
        }

        return count($ids);
    }

    /**
     * Run XTRIM at most every $trimEvery seconds.
     */
    protected function maybeTrim(RedisAdapter $adapter, string $stream, int|string $keep, int &$lastTrim, int $trimEvery): void
    {
        if ($trimEvery <= 0) {
            return;
        }

        $now = time();
        if ($now - $lastTrim < $trimEvery) {
            return;
        }
        $lastTrim = $now;

        try {
            if (is_int($keep)) {
                $adapter->xtrim($stream, 'MAXLEN', '~', $keep);
            } else {
                $threshold = CarbonImmutable::now()
                    ->subMilliseconds((int) CarbonInterval::fromString($keep)->totalMilliseconds)
                    ->getTimestampMs();
                $adapter->xtrim($stream, 'MINID', '~', $threshold);
            }
        } catch (Throwable $e) {
            $this->warn('laraowl:work XTRIM failed: '.$e->getMessage());
        }
    }

    protected function sleepIfNeeded(float $seconds): void
    {
        if ($seconds <= 0) {
            return;
        }
        usleep((int) ($seconds * 1_000_000));
    }

    /**
     * Install SIGTERM/SIGINT handlers if pcntl is available.
     */
    protected function registerSignalHandlers(): void
    {
        if (! function_exists('pcntl_async_signals') || ! function_exists('pcntl_signal')) {
            return;
        }

        pcntl_async_signals(true);
        pcntl_signal(SIGTERM, fn () => $this->shouldStop = true);
        pcntl_signal(SIGINT, fn () => $this->shouldStop = true);
    }
}
