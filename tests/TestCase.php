<?php

namespace Signaladoc\EventBus\Tests;

use Orchestra\Testbench\TestCase as BaseTestCase;
use Signaladoc\EventBus\Consumer\ConsumerServiceProvider;
use Signaladoc\EventBus\Producer\ProducerServiceProvider;

/**
 * Base test case for the event-bus package.
 *
 * Boots a minimal Laravel via Orchestra Testbench, loads both service
 * providers, and configures an in-memory SQLite connection so feature
 * tests can exercise the outbox / receipt-store paths without a real DB.
 */
abstract class TestCase extends BaseTestCase
{
    /**
     * Service providers under test — explicit registration to mirror how
     * consuming services wire the package (auto-discovery is disabled).
     *
     * @return list<class-string>
     */
    protected function getPackageProviders($app): array
    {
        return [
            ProducerServiceProvider::class,
            ConsumerServiceProvider::class,
        ];
    }

    protected function defineEnvironment($app): void
    {
        $app['config']->set('database.default', 'testing');
        $app['config']->set('database.connections.testing', [
            'driver'   => 'sqlite',
            'database' => ':memory:',
            'prefix'   => '',
        ]);
    }
}
