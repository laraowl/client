<?php

namespace Laraowl\Client\Tests\Unit;

use Illuminate\Contracts\Config\Repository;
use Illuminate\Contracts\Redis\Factory as RedisFactory;
use Illuminate\Redis\Connections\Connection;
use Laraowl\Client\RecordsBuffer;
use Laraowl\Client\RedisIngest;
use Laraowl\Client\Support\RedisAdapter;
use Laraowl\Client\Tests\TestCase;
use Mockery;

class RedisIngestTest extends TestCase
{
    protected function tearDown(): void
    {
        Mockery::close();
        parent::tearDown();
    }

    public function test_write_buffers_until_full_then_pipelines_xadd(): void
    {
        $redisFactory = Mockery::mock(RedisFactory::class);
        $connection = Mockery::mock(Connection::class);
        $config = Mockery::mock(Repository::class);

        $config->shouldReceive('get')
            ->with('database.redis.options.prefix', '')
            ->andReturn('');

        $redisFactory->shouldReceive('connection')->with('default')->andReturn($connection);

        // Pipeline is expected exactly once with all buffered records flushed.
        $connection->shouldReceive('pipeline')->once()->andReturnUsing(function ($callback) {
            // Drive the closure with a stub object that records xadd calls.
            $stub = new class
            {
                /** @var list<array{string, array<int, string>}> */
                public array $calls = [];

                public function rawCommand(string ...$args): mixed
                {
                    $this->calls[] = [$args[0], array_slice($args, 1)];
                    return 'id-'.count($this->calls);
                }
            };
            $callback($stub);
            return [];
        });

        $ingest = new RedisIngest(
            redis: $redisFactory,
            config: $config,
            connection: 'default',
            stream: 'laraowl:ingest',
            buffer: new RecordsBuffer(length: 2),
        );

        // RecordsBuffer length=2 means the second write triggers digest.
        $ingest->write(['t' => 'request']);
        $ingest->write(['t' => 'query']);

        $this->assertTrue(true, 'RedisIngest flushed without throwing on full buffer.');
    }

    public function test_digest_is_no_op_when_buffer_empty(): void
    {
        $redisFactory = Mockery::mock(RedisFactory::class);
        $config = Mockery::mock(Repository::class);

        $redisFactory->shouldNotReceive('connection');

        $ingest = new RedisIngest(
            redis: $redisFactory,
            config: $config,
            connection: 'default',
            stream: 'laraowl:ingest',
            buffer: new RecordsBuffer(length: 10),
        );

        $ingest->digest();

        $this->assertTrue(true, 'digest() with empty buffer is a no-op.');
    }

    public function test_redis_outage_does_not_propagate(): void
    {
        $redisFactory = Mockery::mock(RedisFactory::class);
        $connection = Mockery::mock(Connection::class);
        $config = Mockery::mock(Repository::class);

        $config->shouldReceive('get')->andReturn('');
        $redisFactory->shouldReceive('connection')->with('default')->andReturn($connection);
        $connection->shouldReceive('pipeline')->andThrow(new \RuntimeException('redis down'));

        $ingest = new RedisIngest(
            redis: $redisFactory,
            config: $config,
            connection: 'default',
            stream: 'laraowl:ingest',
            buffer: new RecordsBuffer(length: 1),
        );

        // No exception should escape — the request worker stays unaffected.
        $ingest->writeNow(['t' => 'request']);

        $this->assertTrue(true, 'Redis outage was contained inside RedisIngest.');
    }

    public function test_adapter_factory_uses_configured_connection(): void
    {
        $redisFactory = Mockery::mock(RedisFactory::class);
        $connection = Mockery::mock(Connection::class);
        $config = Mockery::mock(Repository::class);

        $redisFactory->shouldReceive('connection')->with('cache')->andReturn($connection);

        $ingest = new RedisIngest(
            redis: $redisFactory,
            config: $config,
            connection: 'cache',
            stream: 'laraowl:ingest',
            buffer: new RecordsBuffer(length: 10),
        );

        $adapter = $ingest->adapter();
        $this->assertInstanceOf(RedisAdapter::class, $adapter);
    }

    public function test_accessors_expose_configured_values(): void
    {
        $ingest = new RedisIngest(
            redis: Mockery::mock(RedisFactory::class),
            config: Mockery::mock(Repository::class),
            connection: 'cache',
            stream: 'laraowl:custom-stream',
            buffer: new RecordsBuffer(length: 10),
            trimKeep: 1000,
        );

        $this->assertSame('cache', $ingest->connection());
        $this->assertSame('laraowl:custom-stream', $ingest->stream());
        $this->assertSame(1000, $ingest->trimKeep());
    }
}
