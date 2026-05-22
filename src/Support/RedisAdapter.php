<?php

namespace Laraowl\Client\Support;

use Illuminate\Contracts\Config\Repository;
use Illuminate\Redis\Connections\Connection;
use Illuminate\Support\Collection;

/**
 * @internal
 *
 * Wraps a Laravel Redis connection so XADD / XRANGE / XDEL / XTRIM commands work
 * uniformly across phpredis, predis, and Relay back-ends by sending raw protocol
 * commands. Inspired by Laravel Pulse's internal RedisAdapter so that contributors
 * already familiar with that codebase find the same shape here.
 */
class RedisAdapter
{
    /**
     * @param  object|null  $client  Optional underlying client (used inside pipelines).
     */
    public function __construct(
        protected Connection $connection,
        protected Repository $config,
        protected ?object $client = null,
    ) {}

    /**
     * Append an entry to a stream and return its generated id.
     *
     * @param  array<string, string>  $dictionary
     */
    public function xadd(string $stream, array $dictionary): mixed
    {
        return $this->handle([
            'XADD',
            $this->prefixed($stream),
            '*',
            ...$this->flattenDictionary($dictionary),
        ]);
    }

    /**
     * Read a range of stream entries keyed by their id.
     *
     * @return array<string, array<string, string>>
     */
    public function xrange(string $stream, string $start, string $end, ?int $count = null): array
    {
        $raw = $this->handle([
            'XRANGE',
            $this->prefixed($stream),
            $start,
            $end,
            ...($count !== null ? ['COUNT', (string) $count] : []),
        ]);

        if (! is_array($raw)) {
            return [];
        }

        $entries = [];

        foreach ($raw as $row) {
            if (! is_array($row) || count($row) < 2) {
                continue;
            }

            [$id, $fields] = $row;

            if (! is_array($fields)) {
                continue;
            }

            $kv = [];
            for ($i = 0, $n = count($fields); $i < $n - 1; $i += 2) {
                $kv[(string) $fields[$i]] = (string) $fields[$i + 1];
            }
            $entries[(string) $id] = $kv;
        }

        return $entries;
    }

    /**
     * Trim a stream.
     */
    public function xtrim(string $stream, string $strategy, string $modifier, string|int $threshold): mixed
    {
        return $this->handle([
            'XTRIM',
            $this->prefixed($stream),
            $strategy,
            $modifier,
            (string) $threshold,
        ]);
    }

    /**
     * Delete entries from a stream.
     *
     * @param  array<int, string>|Collection<int, string>  $ids
     */
    public function xdel(string $stream, array|Collection $ids): mixed
    {
        $ids = $ids instanceof Collection ? $ids->all() : $ids;

        if ($ids === []) {
            return 0;
        }

        return $this->handle([
            'XDEL',
            $this->prefixed($stream),
            ...array_map(strval(...), $ids),
        ]);
    }

    /**
     * Run the closure inside a Redis pipeline. The closure receives a new
     * RedisAdapter wrapping the pipeline client so the same xadd/xtrim helpers
     * can be reused.
     *
     * @param  callable(self): void  $closure
     * @return array<int, mixed>
     */
    public function pipeline(callable $closure): array
    {
        // Different Redis clients pass different concrete pipeline clients into
        // the callback (Predis Pipeline, phpredis Redis, Relay Relay). We accept
        // any of them because the adapter dispatches to rawCommand/executeRaw.
        $callback = function ($client) use ($closure): void {
            if (! is_object($client)) {
                return;
            }
            $closure(new self($this->connection, $this->config, $client));
        };

        // phpstan resolves Connection::pipeline() against the underlying Redis
        // stub (no args). Laravel's actual Connection::pipeline(callable) does
        // take the callback — Pulse uses the same pattern.
        /** @var mixed $result */
        $result = $this->connection->pipeline($callback); // @phpstan-ignore arguments.count

        return is_array($result) ? $result : [];
    }

    /**
     * Prepend Laravel's configured Redis key prefix.
     */
    protected function prefixed(string $key): string
    {
        $prefix = $this->config->get('database.redis.options.prefix', '');

        return (is_string($prefix) ? $prefix : '') . $key;
    }

    /**
     * Flatten ['field' => 'value', ...] into ['field', 'value', ...].
     *
     * @param  array<string, string>  $dictionary
     * @return array<int, string>
     */
    protected function flattenDictionary(array $dictionary): array
    {
        $out = [];
        foreach ($dictionary as $field => $value) {
            $out[] = (string) $field;
            $out[] = (string) $value;
        }
        return $out;
    }

    /**
     * Dispatch a raw command via the underlying client.
     *
     * @param  array<int, string>  $args
     */
    protected function handle(array $args): mixed
    {
        $client = $this->client();

        // phpredis / Relay share a rawCommand($cmd, ...$args) signature.
        if (method_exists($client, 'rawCommand')) {
            return $client->rawCommand(...$args);
        }

        // predis & predis pipeline expose executeRaw(array $args) or
        // executeCommand(RawCommand::create(...)). We try executeRaw first to
        // avoid coupling to Predis's command class loader.
        if (method_exists($client, 'executeRaw')) {
            return $client->executeRaw($args);
        }

        if (method_exists($client, 'executeCommand') && class_exists('Predis\\Command\\RawCommand')) {
            /** @var class-string $rawCommandClass */
            $rawCommandClass = 'Predis\\Command\\RawCommand';

            return $client->executeCommand($rawCommandClass::create(...$args));
        }

        // Fallback: best-effort __call on the Connection. May fail for
        // less-common drivers, in which case the caller should configure a
        // supported Redis client (phpredis, predis, or Relay).
        $method = strtolower($args[0]);
        $arguments = array_slice($args, 1);

        return $this->connection->{$method}(...$arguments);
    }

    /**
     * Resolve the active client (pipeline-aware).
     */
    protected function client(): object
    {
        if ($this->client !== null) {
            return $this->client;
        }

        $client = $this->connection->client();

        if (! is_object($client)) {
            throw new \RuntimeException('Redis connection did not return an object client.');
        }

        return $client;
    }
}
