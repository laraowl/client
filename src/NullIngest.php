<?php

namespace Laraowl\Client;

use Laraowl\Client\Contracts\Ingest as IngestContract;

/**
 * @internal
 *
 * Drops every record on the floor. Useful when LaraOwl is force-disabled (load
 * tests, ephemeral CI containers, vendor smoke tests) and the application
 * should not pay the cost of buffering or transmitting telemetry at all.
 */
final class NullIngest implements IngestContract
{
    public function write(array $record): void {}

    public function writeNow(array $record): void {}

    public function ping(): void {}

    public function shouldDigest(bool $bool = true): void {}

    public function shouldDigestWhenBufferIsFull(bool $bool = true): void {}

    public function digest(): void {}

    public function flush(): void {}
}
