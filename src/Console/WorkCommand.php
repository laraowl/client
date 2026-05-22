<?php

namespace Laraowl\Client\Console;

use Carbon\CarbonImmutable;
use Carbon\CarbonInterval;
use Illuminate\Console\Command;
use Laraowl\Client\HttpIngest;
use Laraowl\Client\RedisIngest;
use Laraowl\Client\Support\RedisAdapter;
use Throwable;

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
        {--trim-every=60 : Run XTRIM at most every N seconds.}';

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

        $adapter = $ingest->adapter();
        $stream = $ingest->stream();
        $lastTrim = 0;

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
    }

    /**
     * Drain at most $chunk entries from the stream. Returns the number of
     * entries actually processed.
     */
    protected function drainOnce(RedisAdapter $adapter, string $stream, int $chunk, HttpIngest $http): int
    {
        try {
            $entries = $adapter->xrange($stream, '-', '+', $chunk);
        } catch (Throwable $e) {
            $this->error('laraowl:work XRANGE failed: ' . $e->getMessage());
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
            try {
                $http->transmit($records);
            } catch (Throwable $e) {
                // HttpIngest already catches+logs internally; this is defense
                // in depth in case a future refactor lets an exception escape.
                $this->error('laraowl:work transmit failed: ' . $e->getMessage());
                return 0; // Do NOT xdel — leave entries for retry.
            }
        }

        try {
            $adapter->xdel($stream, $ids);
        } catch (Throwable $e) {
            // Already transmitted; log and continue. A duplicate transmit on
            // retry is harmless because the server dedupes by fingerprint.
            $this->warn('laraowl:work XDEL failed (records were transmitted): ' . $e->getMessage());
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
            $this->warn('laraowl:work XTRIM failed: ' . $e->getMessage());
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
