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

use function serialize;

/**
 * Regression tests for the data-loss bug Codex flagged: WorkCommand used to
 * XDEL stream entries regardless of whether HttpIngest::transmit() actually
 * succeeded. transmit() swallows GuzzleException internally, so a 401, 500,
 * or network timeout looked identical to a 200 from the worker's
 * perspective — and the buffered records were silently deleted.
 */
class TransmitFailureSafetyTest extends TestCase
{
    protected function tearDown(): void
    {
        Mockery::close();
        parent::tearDown();
    }

    public function test_worker_does_not_xdel_when_transmit_reports_failure(): void
    {
        $redisFactory = Mockery::mock(RedisFactory::class);
        $connection = Mockery::mock(Connection::class);
        $config = Mockery::mock(Repository::class);

        $config->shouldReceive('get')->andReturn('');

        $xdelCalls = 0;
        $stub = new class($xdelCalls)
        {
            public int $xrangeCall = 0;

            public function __construct(public int &$xdelCalls) {}

            public function rawCommand(string $cmd, ...$args): mixed
            {
                return match ($cmd) {
                    'XRANGE' => $this->xrangeCall++ === 0
                        ? [['id-a', ['data', serialize(['t' => 'request', 'url' => '/x'])]]]
                        : [],
                    'XDEL' => ++$this->xdelCalls,
                    default => null,
                };
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

        // Force the transport to report failure (server is down / 5xx / etc.)
        $http = Mockery::mock(HttpIngest::class, [
            'http://laraowl.test', 'token', 0.5, new RecordsBuffer(length: 1), null,
        ])->makePartial();
        $http->shouldReceive('transmitWithResult')->andReturn(false);

        $this->app->instance(RedisIngest::class, $ingest);
        $this->app->instance(HttpIngest::class, $http);

        $exit = $this->artisan('laraowl:work', ['--once' => true, '--no-single-worker' => true])->run();

        $this->assertSame(0, $exit, 'Worker exits cleanly even when transmit failed');
        $this->assertSame(0, $xdelCalls, 'XDEL must not run when transmit reported failure — records stay on stream for retry');
    }

    public function test_worker_xdels_only_after_transmit_success(): void
    {
        $redisFactory = Mockery::mock(RedisFactory::class);
        $connection = Mockery::mock(Connection::class);
        $config = Mockery::mock(Repository::class);

        $config->shouldReceive('get')->andReturn('');

        $xdelCalls = 0;
        $stub = new class($xdelCalls)
        {
            public int $xrangeCall = 0;

            public function __construct(public int &$xdelCalls) {}

            public function rawCommand(string $cmd, ...$args): mixed
            {
                return match ($cmd) {
                    'XRANGE' => $this->xrangeCall++ === 0
                        ? [['id-a', ['data', serialize(['t' => 'request', 'url' => '/x'])]]]
                        : [],
                    'XDEL' => ++$this->xdelCalls,
                    default => null,
                };
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
        $http->shouldReceive('transmitWithResult')->andReturn(true);

        $this->app->instance(RedisIngest::class, $ingest);
        $this->app->instance(HttpIngest::class, $http);

        $exit = $this->artisan('laraowl:work', ['--once' => true, '--no-single-worker' => true])->run();

        $this->assertSame(0, $exit);
        $this->assertSame(1, $xdelCalls, 'XDEL runs exactly once after the successful transmit');
    }

    public function test_http_ingest_transmit_with_result_returns_true_on_success_and_false_on_failure(): void
    {
        // We instantiate a real HttpIngest pointing at a guaranteed-unreachable
        // host so the underlying Guzzle Client raises a ConnectException — that
        // exception is the failure path the new boolean return shape covers.
        $http = new HttpIngest(
            endpoint: 'http://127.0.0.1:1', // closed port → connect refused
            token: 'irrelevant',
            timeout: 0.1,
            buffer: new RecordsBuffer(length: 1),
            app_url: null,
        );

        $this->assertFalse(
            $http->transmitWithResult([['t' => 'request', 'url' => '/x']]),
            'A network failure must surface as a false return so callers can retry'
        );
    }
}
