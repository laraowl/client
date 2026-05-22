<?php

namespace Laraowl\Client;

use GuzzleHttp\Client;
use GuzzleHttp\Exception\GuzzleException;
use Laraowl\Client\Contracts\Ingest as IngestContract;
use Throwable;

use function count;

/**
 * @internal
 *
 * Not marked `final` so downstream applications can extend the transport (for
 * example, wrap transmit() in custom retry/backoff logic) and so test doubles
 * — including Mockery partial mocks — can replace the network boundary.
 */
class HttpIngest implements IngestContract
{
    private bool $shouldDigestWhenBufferIsFull = true;

    public function __construct(
        private string $endpoint,
        private string $token,
        private float $timeout,
        public RecordsBuffer $buffer,
        private ?string $app_url = null,
    ) {
        //
    }

    public function write(array $record): void
    {
        $this->buffer->write($record);

        if ($this->shouldDigestWhenBufferIsFull && $this->buffer->full) {
            $this->digest();
        }
    }

    public function writeNow(array $record): void
    {
        $this->transmit([$record]);
    }

    public function flush(): void
    {
        $this->buffer->flush();
    }

    public function ping(): void
    {
        // Ping could be a health check or just ignored for HTTP
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

        if (empty($records)) {
            return;
        }

        $this->transmit($records);
    }

    /**
     * Transmit one or more records to the LaraOwl server.
     *
     * Exposed publicly so out-of-process daemons (see WorkCommand) can flush
     * batches drained from a Redis stream without re-implementing the HTTP
     * payload shape.
     *
     * This method swallows transport failures (matching original behavior
     * so the host request lifecycle is unaffected by LaraOwl downtime).
     * Daemons that need to know whether the batch reached the server should
     * call {@see self::transmitWithResult()} instead — for example, the
     * `laraowl:work` sidecar uses the boolean return to decide whether
     * acking the Redis stream is safe.
     *
     * @param  array<int, array<mixed>>  $records
     */
    public function transmit(array $records): void
    {
        $this->transmitWithResult($records);
    }

    /**
     * Like {@see self::transmit()} but returns whether the upstream POST
     * actually succeeded. False means the server rejected the batch or the
     * network call failed — callers that buffer records elsewhere (Redis
     * stream, on-disk spool) MUST NOT discard the batch when this returns
     * false, otherwise data is lost silently.
     *
     * @param  array<int, array<mixed>>  $records
     */
    public function transmitWithResult(array $records): bool
    {
        $client = new Client([
            'base_uri' => $this->endpoint,
            'timeout' => $this->timeout,
        ]);

        try {
            \Illuminate\Support\Facades\Log::info('Laraowl Attempting to transmit '.count($records).' records to '.$this->endpoint);
            $client->post('/api/records', [
                'headers' => [
                    'Authorization' => 'Bearer '.$this->token,
                    'Accept' => 'application/json',
                ],
                'json' => [
                    'app_url' => $this->app_url,
                    'records' => $records,
                ],
            ]);

            return true;
        } catch (GuzzleException|Throwable $e) {
            \Illuminate\Support\Facades\Log::error('Laraowl Ingest Error: '.$e->getMessage()."\n".$e->getTraceAsString());

            return false;
        }
    }
}
