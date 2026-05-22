<?php

namespace Laraowl\Client\Tests\Unit;

use Laraowl\Client\NullIngest;
use Laraowl\Client\Tests\TestCase;

class NullIngestTest extends TestCase
{
    public function test_every_method_is_a_safe_noop(): void
    {
        $ingest = new NullIngest();

        // Should not throw on any record shape or call.
        $ingest->write(['t' => 'request', 'v' => 1]);
        $ingest->writeNow(['t' => 'exception']);
        $ingest->ping();
        $ingest->shouldDigest(true);
        $ingest->shouldDigestWhenBufferIsFull(false);
        $ingest->digest();
        $ingest->flush();

        $this->assertTrue(true, 'NullIngest never throws.');
    }
}
