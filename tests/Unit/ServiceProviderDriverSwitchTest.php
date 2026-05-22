<?php

namespace Laraowl\Client\Tests\Unit;

use Laraowl\Client\HttpIngest;
use Laraowl\Client\NullIngest;
use Laraowl\Client\RedisIngest;
use Laraowl\Client\Tests\TestCase;

class ServiceProviderDriverSwitchTest extends TestCase
{
    public function test_http_driver_is_default(): void
    {
        $this->app['config']->set('laraowl.ingest.driver', null);

        // Re-register provider after config tweak
        $this->refreshIngestRegistration();

        $this->assertInstanceOf(HttpIngest::class, $this->app->make(HttpIngest::class));
    }

    public function test_redis_driver_registers_redis_and_http(): void
    {
        $this->app['config']->set('laraowl.ingest.driver', 'redis');
        $this->app['config']->set('laraowl.ingest.redis.connection', 'default');
        $this->app['config']->set('laraowl.ingest.redis.stream', 'laraowl:ingest');

        $this->refreshIngestRegistration();

        $this->assertInstanceOf(RedisIngest::class, $this->app->make(RedisIngest::class));
        // HttpIngest is still registered — the sidecar reuses it as outbound transport.
        $this->assertInstanceOf(HttpIngest::class, $this->app->make(HttpIngest::class));
    }

    public function test_null_driver_registers_null_ingest_and_skips_http(): void
    {
        $this->app['config']->set('laraowl.ingest.driver', 'null');

        $this->refreshIngestRegistration();

        $this->assertInstanceOf(NullIngest::class, $this->app->make(NullIngest::class));
    }

    public function test_unknown_driver_falls_back_to_http(): void
    {
        $this->app['config']->set('laraowl.ingest.driver', 'completely-made-up');

        $this->refreshIngestRegistration();

        $this->assertInstanceOf(HttpIngest::class, $this->app->make(HttpIngest::class));
    }

    /**
     * Tear down + re-bootstrap the provider so the driver switch runs again
     * against the updated config snapshot.
     */
    private function refreshIngestRegistration(): void
    {
        $provider = new \Laraowl\Client\LaraowlClientServiceProvider($this->app);
        $provider->register();
        $provider->boot();
    }
}
