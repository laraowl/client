<?php

namespace Laraowl\Client\Contracts;

use Deprecated;

/**
 * @internal
 */
interface Ingest
{
    /**
     * @param  array<mixed>  $record
     */
    public function write(array $record): void;

    /**
     * @param  array<mixed>  $record
     */
    public function writeNow(array $record): void;

    /**
     * Transmit an already-bufferred batch of records immediately, bypassing
     * the buffer. Used by the queued job that defers transmission.
     *
     * @param  array<int, array<mixed>>  $records
     */
    public function transmitBatch(array $records): void;

    public function ping(): void;

    #[Deprecated('Use shouldDigestWhenBufferIsFull instead')]
    public function shouldDigest(bool $bool = true): void;

    public function shouldDigestWhenBufferIsFull(bool $bool = true): void;

    public function digest(): void;

    public function flush(): void;
}
