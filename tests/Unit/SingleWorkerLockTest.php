<?php

namespace Laraowl\Client\Tests\Unit;

use Illuminate\Contracts\Config\Repository;
use Illuminate\Contracts\Redis\Factory as RedisFactory;
use Illuminate\Redis\Connections\Connection;
use Illuminate\Support\Facades\Cache;
use Laraowl\Client\HttpIngest;
use Laraowl\Client\RecordsBuffer;
use Laraowl\Client\RedisIngest;
use Laraowl\Client\Tests\TestCase;
use Mockery;

/**
 * Codex flagged that XRANGE/XDEL has no consumer-group semantics, so two
 * laraowl:work daemons on the same stream can both read and transmit the
 * same entries before either deletes them. The fix is a non-blocking
 * cluster-wide Cache::lock keyed on the stream name; siblings exit cleanly.
 */
class SingleWorkerLockTest extends TestCase
{
    protected function tearDown(): void
    {
        Mockery::close();
        parent::tearDown();
    }

    public function test_second_worker_exits_clean_when_lock_held(): void
    {
        // Hold the lock under a different owner so the worker cannot acquire it.
        $lock = Cache::lock('laraowl:work:laraowl:ingest', 0);
        $this->assertTrue($lock->get(), 'Pretend an external worker already owns the drain lock');

        try {
            $redisFactory = Mockery::mock(RedisFactory::class);
            $config = Mockery::mock(Repository::class);
            // The worker should NEVER reach XRANGE because it cannot acquire the lock.
            $redisFactory->shouldNotReceive('connection');

            $ingest = new RedisIngest(
                redis: $redisFactory,
                config: $config,
                connection: 'default',
                stream: 'laraowl:ingest',
                buffer: new RecordsBuffer(length: 10),
            );
            $http = Mockery::mock(HttpIngest::class, [
                'http://laraowl.test', 'token', 0.5, new RecordsBuffer(length: 1), null,
            ])->makePartial();
            $http->shouldNotReceive('transmit');
            $http->shouldNotReceive('transmitWithResult');

            $this->app->instance(RedisIngest::class, $ingest);
            $this->app->instance(HttpIngest::class, $http);

            $exit = $this->artisan('laraowl:work', ['--once' => true])->run();

            $this->assertSame(0, $exit, 'Sibling worker exits cleanly when the lock is taken');
        } finally {
            $lock->release();
        }
    }

    public function test_no_single_worker_flag_bypasses_the_lock(): void
    {
        // Hold the lock first.
        $lock = Cache::lock('laraowl:work:laraowl:ingest', 0);
        $this->assertTrue($lock->get());

        try {
            $redisFactory = Mockery::mock(RedisFactory::class);
            $connection = Mockery::mock(Connection::class);
            $config = Mockery::mock(Repository::class);

            $config->shouldReceive('get')->andReturn('');

            $xrangeCalls = 0;
            $stub = new class($xrangeCalls)
            {
                public function __construct(public int &$count) {}

                public function rawCommand(string $cmd, ...$args): mixed
                {
                    if ($cmd === 'XRANGE') {
                        $this->count++;
                    }

                    return [];
                }
            };
            $connection->shouldReceive('client')->andReturn($stub);
            $redisFactory->shouldReceive('connection')->andReturn($connection);

            $ingest = new RedisIngest(
                redis: $redisFactory,
                config: $config,
                connection: 'default',
                stream: 'laraowl:ingest',
                buffer: new RecordsBuffer(length: 10),
            );
            $http = Mockery::mock(HttpIngest::class, [
                'http://laraowl.test', 'token', 0.5, new RecordsBuffer(length: 1), null,
            ])->makePartial();

            $this->app->instance(RedisIngest::class, $ingest);
            $this->app->instance(HttpIngest::class, $http);

            $exit = $this->artisan('laraowl:work', ['--once' => true, '--no-single-worker' => true])->run();

            $this->assertSame(0, $exit);
            $this->assertSame(1, $xrangeCalls, 'With --no-single-worker the lock is bypassed and XRANGE still runs');
        } finally {
            $lock->release();
        }
    }
}
