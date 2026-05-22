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

use function array_keys;
use function array_map;
use function array_merge;
use function array_values;
use function count;
use function serialize;

class WorkCommandTest extends TestCase
{
    protected function tearDown(): void
    {
        Mockery::close();
        parent::tearDown();
    }

    public function test_once_mode_drains_existing_records_and_exits(): void
    {
        $redisFactory = Mockery::mock(RedisFactory::class);
        $connection = Mockery::mock(Connection::class);
        $config = Mockery::mock(Repository::class);

        $config->shouldReceive('get')
            ->with('database.redis.options.prefix', '')
            ->andReturn('');

        // 1st xrange call returns two serialized records; 2nd call returns empty.
        $serialized = [
            'a-id' => ['data' => serialize(['t' => 'request', 'url' => '/api/x'])],
            'b-id' => ['data' => serialize(['t' => 'query', 'sql' => 'select 1'])],
        ];

        $rawCommands = [];
        $stub = new class($rawCommands, $serialized)
        {
            public int $xrangeCall = 0;

            public function __construct(public array &$calls, public array $serialized) {}

            public function rawCommand(string $cmd, ...$args): mixed
            {
                $this->calls[] = $cmd;

                return match ($cmd) {
                    'XRANGE' => $this->xrangeCall++ === 0
                        ? $this->asLegacyArray($this->serialized)
                        : [],
                    'XDEL' => count($args) - 1, // pretend deleted N
                    'XTRIM' => 0,
                    default => null,
                };
            }

            /**
             * Convert ['id' => ['data' => $blob], ...] into the
             * [[id, [field, value, ...]], ...] shape that phpredis XRANGE returns.
             */
            private function asLegacyArray(array $entries): array
            {
                $out = [];
                foreach ($entries as $id => $fields) {
                    $flat = [];
                    foreach ($fields as $k => $v) {
                        $flat[] = $k;
                        $flat[] = $v;
                    }
                    $out[] = [$id, $flat];
                }

                return $out;
            }
        };

        $connection->shouldReceive('client')->andReturn($stub);

        $redisFactory->shouldReceive('connection')->with('default')->andReturn($connection);

        $ingest = new RedisIngest(
            redis: $redisFactory,
            config: $config,
            connection: 'default',
            stream: 'laraowl:ingest',
            buffer: new RecordsBuffer(length: 10),
        );

        // Stub HttpIngest::transmitWithResult so we can assert it received the records.
        $http = Mockery::mock(HttpIngest::class, [
            'http://laraowl.test',
            'token',
            0.5,
            new RecordsBuffer(length: 1),
            null,
        ])->makePartial();

        $captured = [];
        $http->shouldReceive('transmitWithResult')->andReturnUsing(function (array $records) use (&$captured): bool {
            $captured = $records;

            return true;
        });

        $this->app->instance(RedisIngest::class, $ingest);
        $this->app->instance(HttpIngest::class, $http);

        $exitCode = $this->artisan('laraowl:work', ['--once' => true, '--chunk' => 50])->run();

        $this->assertSame(0, $exitCode, 'Command exits successfully in --once mode');

        $this->assertCount(2, $captured, 'HttpIngest::transmit received both records');
        $this->assertSame('request', $captured[0]['t'] ?? null);
        $this->assertSame('query', $captured[1]['t'] ?? null);

        // We expect XRANGE → XDEL after successful transmit
        $this->assertContains('XRANGE', $rawCommands);
        $this->assertContains('XDEL', $rawCommands);
    }

    public function test_unserializable_payload_is_skipped_safely(): void
    {
        $redisFactory = Mockery::mock(RedisFactory::class);
        $connection = Mockery::mock(Connection::class);
        $config = Mockery::mock(Repository::class);

        $config->shouldReceive('get')->andReturn('');

        $serialized = [
            'bad-id' => ['data' => 'this is not a valid serialized payload'],
        ];

        $stub = new class($serialized)
        {
            public int $xrangeCall = 0;

            public function __construct(public array $serialized) {}

            public function rawCommand(string $cmd, ...$args): mixed
            {
                return match ($cmd) {
                    'XRANGE' => $this->xrangeCall++ === 0
                        ? array_map(
                            fn ($id, $fields) => [$id, array_merge(...array_map(fn ($k, $v) => [$k, $v], array_keys($fields), array_values($fields)))],
                            array_keys($this->serialized),
                            $this->serialized
                        )
                        : [],
                    'XDEL', 'XTRIM' => 0,
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

        $http = Mockery::mock(HttpIngest::class)->makePartial();
        // Should not be called because no valid records were extracted.
        $http->shouldNotReceive('transmit');
        $http->shouldNotReceive('transmitWithResult');

        $this->app->instance(RedisIngest::class, $ingest);
        $this->app->instance(HttpIngest::class, $http);

        $exitCode = $this->artisan('laraowl:work', ['--once' => true])->run();

        $this->assertSame(0, $exitCode);
    }
}
