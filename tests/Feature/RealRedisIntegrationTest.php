<?php

namespace Laraowl\Client\Tests\Feature;

use Illuminate\Support\Facades\Cache;
use Laraowl\Client\HttpIngest;
use Laraowl\Client\RecordsBuffer;
use Laraowl\Client\RedisIngest;
use Laraowl\Client\Tests\TestCase;
use Mockery;
use Throwable;

use function array_key_first;
use function env;
use function uniqid;

/**
 * Real Redis integration: spin up a Predis client against a live Redis on
 * localhost:6379 and prove the end-to-end "RedisIngest write → laraowl:work
 * drain → HTTP transmit → XDEL" pipeline actually behaves correctly with
 * the real stream semantics, not just the stubs the unit tests use.
 *
 * Skipped automatically when Redis is not reachable so CI environments
 * without a local Redis stay green.
 */
class RealRedisIntegrationTest extends TestCase
{
    protected function defineEnvironment($app): void
    {
        parent::defineEnvironment($app);
        $app['config']->set('database.redis.client', 'predis');
        $app['config']->set('database.redis.default', [
            'host' => env('REDIS_HOST', '127.0.0.1'),
            'port' => env('REDIS_PORT', 6379),
            'database' => 15, // dedicated test database
        ]);
        $app['config']->set('cache.default', 'array');
    }

    protected function setUp(): void
    {
        parent::setUp();
        $this->markIfRedisUnavailable();
        $this->wipeTestStream();
    }

    protected function tearDown(): void
    {
        if ($this->isRedisAvailable()) {
            $this->wipeTestStream();
        }
        Mockery::close();
        parent::tearDown();
    }

    public function test_records_flow_through_real_redis_stream_to_http_transmit(): void
    {
        $stream = 'laraowl:test:flow:'.uniqid('', true);

        $ingest = new RedisIngest(
            redis: $this->app['redis'],
            config: $this->app['config'],
            connection: 'default',
            stream: $stream,
            buffer: new RecordsBuffer(length: 100),
        );

        // 1. Write four records of varied shapes — all should land on the
        //    stream as a single pipelined XADD batch.
        $ingest->write(['t' => 'request', 'method' => 'GET', 'route_path' => '/api/x']);
        $ingest->write(['t' => 'exception', 'class' => 'RuntimeException', 'message' => 'boom']);
        $ingest->write(['t' => 'query', 'sql' => 'select 1']);
        $ingest->write(['t' => 'command', 'command' => 'app:test', 'duration' => 250]);
        $ingest->digest();

        // 2. Redis should now have four entries on the stream.
        $entries = $this->app['redis']
            ->connection('default')
            ->xrange($stream, '-', '+');
        $this->assertCount(4, $entries, 'RedisIngest XADDed four entries to the real Redis stream');

        // 3. Run laraowl:work --once against the same stream with a captured
        //    HttpIngest so we can assert the wire payload reaching the server.
        $captured = [];
        $http = Mockery::mock(HttpIngest::class, [
            'http://laraowl.test', 'token', 0.5, new RecordsBuffer(length: 1), null,
        ])->makePartial();
        $http->shouldReceive('transmitWithResult')->andReturnUsing(function (array $records) use (&$captured): bool {
            $captured = $records;

            return true;
        });

        $this->app['config']->set('laraowl.ingest.redis.stream', $stream);
        $this->app->instance(RedisIngest::class, $ingest);
        $this->app->instance(HttpIngest::class, $http);

        $this->artisan('laraowl:work', ['--once' => true, '--no-single-worker' => true])
            ->assertExitCode(0);

        // 4. WorkCommand should have transmitted all four records and XDELed
        //    them from the stream.
        $this->assertCount(4, $captured, 'transmit received all four buffered records');
        $this->assertSame('request', $captured[0]['t']);
        $this->assertSame('exception', $captured[1]['t']);

        $afterDrain = $this->app['redis']
            ->connection('default')
            ->xrange($stream, '-', '+');
        $this->assertCount(0, $afterDrain, 'XDEL ran on success, stream is empty');
    }

    public function test_failed_transmit_leaves_entries_for_retry_on_real_redis(): void
    {
        $stream = 'laraowl:test:retry:'.uniqid('', true);

        $ingest = new RedisIngest(
            redis: $this->app['redis'],
            config: $this->app['config'],
            connection: 'default',
            stream: $stream,
            buffer: new RecordsBuffer(length: 100),
        );

        $ingest->write(['t' => 'request', 'method' => 'POST', 'route_path' => '/checkout']);
        $ingest->digest();

        $before = $this->app['redis']->connection('default')->xrange($stream, '-', '+');
        $this->assertCount(1, $before);

        // Force transmit failure (server down / 5xx).
        $http = Mockery::mock(HttpIngest::class, [
            'http://laraowl.test', 'token', 0.5, new RecordsBuffer(length: 1), null,
        ])->makePartial();
        $http->shouldReceive('transmitWithResult')->andReturn(false);

        $this->app['config']->set('laraowl.ingest.redis.stream', $stream);
        $this->app->instance(RedisIngest::class, $ingest);
        $this->app->instance(HttpIngest::class, $http);

        $this->artisan('laraowl:work', ['--once' => true, '--no-single-worker' => true])
            ->assertExitCode(0);

        // CRITICAL: the entry MUST still be on the stream — XDEL was skipped
        // because transmit reported failure. This is the Codex #1 regression.
        $after = $this->app['redis']->connection('default')->xrange($stream, '-', '+');
        $this->assertCount(1, $after, 'Failed transmit must NOT delete entries from the stream');
        $this->assertSame((string) $before[array_key_first($before)]['data'] ?? '', (string) $after[array_key_first($after)]['data'] ?? '');
    }

    public function test_single_worker_lock_keeps_a_sibling_drainer_from_xrange(): void
    {
        $stream = 'laraowl:test:singleworker:'.uniqid('', true);

        $ingest = new RedisIngest(
            redis: $this->app['redis'],
            config: $this->app['config'],
            connection: 'default',
            stream: $stream,
            buffer: new RecordsBuffer(length: 100),
        );

        $ingest->write(['t' => 'query', 'sql' => 'select 2']);
        $ingest->digest();

        // Pretend another worker holds the cluster-wide lock for this stream.
        $heldLock = Cache::lock('laraowl:work:'.$stream, 0);
        $this->assertTrue($heldLock->get(), 'External worker took the lock first');

        try {
            $http = Mockery::mock(HttpIngest::class, [
                'http://laraowl.test', 'token', 0.5, new RecordsBuffer(length: 1), null,
            ])->makePartial();
            $http->shouldNotReceive('transmit');
            $http->shouldNotReceive('transmitWithResult');

            $this->app['config']->set('laraowl.ingest.redis.stream', $stream);
            $this->app->instance(RedisIngest::class, $ingest);
            $this->app->instance(HttpIngest::class, $http);

            $this->artisan('laraowl:work', ['--once' => true])->assertExitCode(0);

            // Sibling worker exited without touching Redis, so the entry stays.
            $stillThere = $this->app['redis']->connection('default')->xrange($stream, '-', '+');
            $this->assertCount(1, $stillThere, 'Sibling worker must not have drained the stream');
        } finally {
            $heldLock->release();
        }
    }

    private function isRedisAvailable(): bool
    {
        try {
            return $this->app['redis']->connection('default')->ping() !== null;
        } catch (Throwable) {
            return false;
        }
    }

    private function markIfRedisUnavailable(): void
    {
        if (! $this->isRedisAvailable()) {
            $this->markTestSkipped('Redis not reachable at '.env('REDIS_HOST', '127.0.0.1').':'.env('REDIS_PORT', 6379));
        }
    }

    private function wipeTestStream(): void
    {
        try {
            $keys = $this->app['redis']->connection('default')->keys('laraowl:test:*');
            if (! empty($keys)) {
                $this->app['redis']->connection('default')->del($keys);
            }
        } catch (Throwable) {
            // best-effort
        }
    }
}
