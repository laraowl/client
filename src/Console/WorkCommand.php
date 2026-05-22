<?php

namespace Laraowl\Client\Console;

use Carbon\CarbonImmutable;
use Carbon\CarbonInterval;
use Illuminate\Console\Command;
use Illuminate\Support\Facades\Cache;
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
        $stream = $ingest->stream();
        $lastTrim = 0;

        // Cluster-wide single-worker guard. XRANGE/XDEL is not safe across
        // multiple drainers because two workers can read the same entries
        // before either deletes them — leading to duplicated transmit().
        // A non-blocking Cache::lock keyed on the stream name lets only one
        // sidecar actually drain at a time; siblings exit cleanly so the
        // operator can run them in HA/replicated mode.
        //
        // The lock is acquired BEFORE the Redis adapter is resolved so a
        // sibling worker is rejected before it touches Redis at all.
        //
        // Pass --no-single-worker to bypass when an external consumer-group
        // scheme is in place.
        $lock = null;
        if (! $skipLock) {
            try {
                $lock = Cache::lock('laraowl:work:'.$stream, 0);
                if (! $lock->get()) {
                    $this->info('laraowl:work another drainer holds the lock for stream ['.$stream.']; exiting.');

                    return self::SUCCESS;
                }
            } catch (Throwable $e) {
                // Cache layer unavailable: fall back to drain without lock
                // rather than block telemetry entirely.
                $this->warn('laraowl:work could not acquire single-worker lock ('.$e->getMessage().'); proceeding without it.');
                $lock = null;
            }
        }

        $adapter = $ingest->adapter();

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

                if ($maxTime > 0 && CarbonImmutable::now()->diffInSeconds($start) >= $maxTime) {
                    $this->info(sprintf('laraowl:work hit --max-time=%ds, exiting.', $maxTime));

                    return self::SUCCESS;
                }
            }

            $this->info('laraowl:work received shutdown signal, exiting.');

            return self::SUCCESS;
        } finally {
            if ($lock !== null) {
                try {
                    $lock->release();
                } catch (Throwable) {
                    // Lock release is best-effort; the TTL of 0 means it's
                    // already gone the next time another worker tries.
                }
            }
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
