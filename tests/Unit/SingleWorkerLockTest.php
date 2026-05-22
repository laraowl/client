<?php

namespace Laraowl\Client\Tests\Unit;

use Illuminate\Contracts\Config\Repository;
use Illuminate\Contracts\Redis\Factory as RedisFactory;
use Illuminate\Redis\Connections\Connection;
use Laraowl\Client\HttpIngest;
use Laraowl\Client\RecordsBuffer;
use Laraowl\Client\RedisIngest;
use Laraowl\Client\Tests\TestCase;
use Mockery;

use function array_merge;
use function in_array;

/**
 * Codex flagged that XRANGE/XDEL has no consumer-group semantics, so two
 * laraowl:work daemons on the same stream can both read and transmit the
 * same entries before either deletes them.
 *
 * The fix (in WorkCommand) acquires a finite-TTL Redis-backed lock keyed on
 * the stream name BEFORE draining. The lock lives on the same Redis as the
 * stream — not on the host application's Laravel cache — so it stays
 * consistent across distributed workers and survives a host whose cache
 * driver is array/local.
 */
class SingleWorkerLockTest extends TestCase
{
    protected function tearDown(): void
    {
        Mockery::close();
        parent::tearDown();
    }

    public function test_sibling_worker_exits_clean_when_redis_lock_acquire_returns_false(): void
    {
        // Simulate "another worker already holds the lock": SET NX EX returns
        // null (predis) or false (phpredis), which lockAcquire() maps to false.
        $stub = $this->makeRedisClientStub(setNxResult: null);

        $exit = $this->runWorkerWithStub($stub);

        $this->assertSame(0, $exit, 'Sibling worker exits cleanly when SET NX EX fails (another holder owns the key)');
        $this->assertSame(0, $stub->xrangeCalls, 'Worker never reached XRANGE — lock rejection happened first');
    }

    public function test_worker_drains_when_lock_acquire_succeeds(): void
    {
        // SET NX EX returns "OK" → lockAcquire() returns true → drain proceeds.
        $stub = $this->makeRedisClientStub(setNxResult: 'OK');

        $exit = $this->runWorkerWithStub($stub);

        $this->assertSame(0, $exit);
        $this->assertSame(1, $stub->xrangeCalls, 'Worker drained the stream after winning the lock');
        $this->assertGreaterThanOrEqual(1, $stub->evalCalls, 'Worker released the lock via EVAL on the way out');
    }

    public function test_no_single_worker_flag_skips_lock_acquire(): void
    {
        $stub = $this->makeRedisClientStub(setNxResult: null);

        $exit = $this->runWorkerWithStub($stub, ['--no-single-worker' => true]);

        $this->assertSame(0, $exit);
        $this->assertSame(0, $stub->setNxCalls, '--no-single-worker bypasses SET NX entirely');
        $this->assertSame(1, $stub->xrangeCalls, 'XRANGE still runs when the lock is bypassed');
    }

    /**
     * Build an anonymous Redis-client stub that simulates the protocol commands
     * WorkCommand sends. Captures call counts so the assertions above stay
     * tight against behavior, not implementation detail.
     */
    private function makeRedisClientStub(mixed $setNxResult): object
    {
        return new class($setNxResult)
        {
            public int $xrangeCalls = 0;

            public int $setNxCalls = 0;

            public int $evalCalls = 0;

            public function __construct(public mixed $setNxResult) {}

            public function rawCommand(string $cmd, ...$args): mixed
            {
                return match ($cmd) {
                    'SET' => $this->handleSet($args),
                    'EVAL' => ++$this->evalCalls,
                    'XRANGE' => $this->handleXrange(),
                    'XDEL', 'XTRIM' => 0,
                    default => null,
                };
            }

            private function handleSet(array $args): mixed
            {
                // Only count SET ... NX EX as a lock attempt.
                if (in_array('NX', $args, true)) {
                    $this->setNxCalls++;

                    return $this->setNxResult;
                }

                return 'OK';
            }

            private function handleXrange(): array
            {
                $this->xrangeCalls++;

                return []; // empty stream → worker exits after one drain
            }
        };
    }

    /**
     * @param  array<string, mixed>  $extraOpts
     */
    private function runWorkerWithStub(object $stub, array $extraOpts = []): int
    {
        $redisFactory = Mockery::mock(RedisFactory::class);
        $connection = Mockery::mock(Connection::class);
        $config = Mockery::mock(Repository::class);

        $config->shouldReceive('get')->with('database.redis.options.prefix', '')->andReturn('');
        $config->shouldReceive('get')->andReturn('');
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
        // No records on the stub stream → no transmit attempted.
        $http->shouldNotReceive('transmit');
        $http->shouldNotReceive('transmitWithResult');

        $this->app->instance(RedisIngest::class, $ingest);
        $this->app->instance(HttpIngest::class, $http);

        return $this->artisan('laraowl:work', array_merge(['--once' => true], $extraOpts))->run();
    }
}
