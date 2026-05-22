<?php

namespace Laraowl\Client\Tests;

use Laraowl\Client\LaraowlClientServiceProvider;
use Orchestra\Testbench\TestCase as Orchestra;

abstract class TestCase extends Orchestra
{
    /**
     * @return array<int, class-string>
     */
    protected function getPackageProviders($app): array
    {
        return [LaraowlClientServiceProvider::class];
    }

    protected function defineEnvironment($app): void
    {
        // Disable the client by default; individual tests opt-in to ingestion.
        $app['config']->set('laraowl.enabled', false);
        $app['config']->set('laraowl.token', 'test-token');
        $app['config']->set('laraowl.server_url', 'http://laraowl.test');
    }
}
