<?php

namespace Laraowl\Client;

use Illuminate\Contracts\Config\Repository;
use Illuminate\Contracts\Redis\Factory as RedisFactory;
use Illuminate\Support\Facades\Log;
use Laraowl\Client\Contracts\Ingest as IngestContract;
use Laraowl\Client\Support\RedisAdapter;
use Throwable;

/**
 * @internal
 *
 * Pushes raw records into a Redis stream so the HTTP transmit cost does not
 * block the request worker. A separate sidecar process drains the stream and
 * forwards batched records to the LaraOwl server via HttpIngest.
 */
final class RedisIngest implements IngestContract
{
    private bool $shouldDigestWhenBufferIsFull = true;

    /**
     * @param  RedisFactory  $redis  Laravel Redis factory used to resolve the connection at call-time.
     * @param  Repository  $config  Application config repository (needed by the adapter for key prefixing).
     * @param  string  $connection  Redis connection name configured in `config/database.php`.
     * @param  string  $stream  Redis stream key where buffered records are appended.
     * @param  int|string  $trimKeep  XTRIM threshold: int → MAXLEN entries; string → MINID interval (e.g. "5 minutes").
     */
    public function __construct(
        private RedisFactory $redis,
        private Repository $config,
        private string $connection,
        private string $stream,
        public RecordsBuffer $buffer,
        private int|string $trimKeep = '5 minutes',
    ) {}

    public function write(array $record): void
    {
        $this->buffer->write($record);

        if ($this->shouldDigestWhenBufferIsFull && $this->buffer->full) {
            $this->digest();
        }
    }

    public function writeNow(array $record): void
    {
        $this->push([$record]);
    }

    public function flush(): void
    {
        $this->buffer->flush();
    }

    public function ping(): void
    {
        // No-op for the Redis transport. The sidecar daemon owns liveness
        // against the upstream HTTP endpoint.
    }

    public function shouldDigest(bool $bool = true): void
    {
        $this->shouldDigestWhenBufferIsFull($bool);
    }

    public function shouldDigestWhenBufferIsFull(bool $bool = true): void
    {
        $this->shouldDigestWhenBufferIsFull = $bool;
    }

    public function digest(): void
    {
        $records = $this->buffer->pullRaw();

        if ($records === []) {
            return;
        }

        $this->push($records);
    }

    /**
     * @param  array<int, array<mixed>>  $records
     */
    private function push(array $records): void
    {
        try {
            $adapter = $this->adapter();
            $stream = $this->stream;

            $adapter->pipeline(function (RedisAdapter $pipe) use ($records, $stream): void {
                foreach ($records as $record) {
                    $pipe->xadd($stream, [
                        'data' => serialize($this->stripUnserializable($record)),
                    ]);
                }
            });
        } catch (Throwable $e) {
            // Match the silent-log behavior of HttpIngest so a Redis outage does
            // not propagate into the host request lifecycle.
            Log::error('Laraowl Redis Ingest Error: ' . $e->getMessage(), [
                'exception' => $e,
            ]);
        }
    }

    /**
     * Strip Closures, resources, and non-JSON-encodable values from a record
     * so it can be passed through `serialize()` safely.
     *
     * Laravel sensors and host application listeners occasionally surface
     * payloads that contain Closures (logger context, view composers, queued
     * jobs, etc.). The original HttpIngest path delivered records via Guzzle's
     * JSON body which silently dropped these. Redis stream entries are passed
     * through `serialize()` instead — which throws `Serialization of 'Closure'
     * is not allowed` and causes the whole batch to be lost.
     *
     * A JSON round-trip with `JSON_PARTIAL_OUTPUT_ON_ERROR` matches the
     * legacy lossy-but-safe behavior: unserializable leaves become `null`
     * but the rest of the record reaches the sidecar intact.
     *
     * @param  array<mixed>  $record
     * @return array<mixed>
     */
    private function stripUnserializable(array $record): array
    {
        $encoded = json_encode($record, JSON_PARTIAL_OUTPUT_ON_ERROR | JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES | JSON_INVALID_UTF8_SUBSTITUTE);

        if ($encoded === false) {
            return [];
        }

        $decoded = json_decode($encoded, true);

        return is_array($decoded) ? $decoded : [];
    }

    /**
     * Return the configured stream name (used by the work command).
     */
    public function stream(): string
    {
        return $this->stream;
    }

    /**
     * Return the configured Redis connection name (used by the work command).
     */
    public function connection(): string
    {
        return $this->connection;
    }

    /**
     * Build a RedisAdapter for the configured connection.
     */
    public function adapter(): RedisAdapter
    {
        return new RedisAdapter(
            $this->redis->connection($this->connection),
            $this->config,
        );
    }

    /**
     * Return the trim policy. Used by the work command to keep the stream bounded.
     */
    public function trimKeep(): int|string
    {
        return $this->trimKeep;
    }
}
