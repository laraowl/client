<?php

namespace Laraowl\Client\Tests\Unit;

use Illuminate\Config\Repository;
use Illuminate\Container\Container;
use Illuminate\Contracts\Bus\Dispatcher;
use Illuminate\Support\Facades\Facade;
use Laraowl\Client\Compatibility;
use Laraowl\Client\HttpIngest;
use Laraowl\Client\Jobs\TransmitRecords;
use Laraowl\Client\LazyValue;
use Laraowl\Client\RecordsBuffer;
use PHPUnit\Framework\TestCase;
use ReflectionProperty;
use RuntimeException;

use function putenv;
use function serialize;

/**
 * Exercises HttpIngest::digest() without booting a full Laravel application.
 * Only the `config` binding and the Facade application are set up manually,
 * which is enough for the `config()`/`now()` helpers used by digest() to
 * resolve; a fake Bus dispatcher captures whatever job would have been sent
 * to the queue.
 */
final class HttpIngestQueueTest extends TestCase
{
    /** @var array<int, object> */
    public array $dispatchedJobs = [];

    /** @var array<int, string> */
    public array $loggedErrors = [];

    /** @var array<int, bool|null> */
    public array $samplingDuringDispatch = [];

    protected function setUp(): void
    {
        parent::setUp();

        $this->dispatchedJobs = [];
        $this->loggedErrors = [];
        $this->samplingDuringDispatch = [];

        $container = new Container;
        Container::setInstance($container);

        $this->rebindConfig();
        $this->bindDispatcher(function ($command) {
            $this->dispatchedJobs[] = $command;
            // Captured here rather than after digest() returns, because
            // dispatchUnsampled() restores the previous value on the way out.
            $this->samplingDuringDispatch[] = Compatibility::getSamplingFromContext(null);
        });

        // HttpIngest::transmitBatch() and the queue-failure branch both log
        // via the Log facade; capture the messages so the tests can assert on
        // them instead of letting the facade fail to resolve.
        $test = $this;
        $container->instance('log', new class($test)
        {
            public function __construct(private HttpIngestQueueTest $test) {}

            public function error($message, array $context = []): void
            {
                $this->test->loggedErrors[] = (string) $message;
            }
        });

        Facade::setFacadeApplication($container);
    }

    protected function tearDown(): void
    {
        Facade::clearResolvedInstances();
        // Facade::$app is static: without this it keeps pointing at the
        // container discarded below (fake `log` binding included) for every
        // later test in the same PHPUnit process.
        Facade::setFacadeApplication(null);
        Container::setInstance(null);
        Compatibility::removeSamplingFromContext();
        putenv('LARAOWL_QUEUE_CONNECTION');
        putenv('LARAOWL_QUEUE');

        parent::tearDown();
    }

    public function test_it_dispatches_a_queued_job_when_a_queue_connection_is_configured(): void
    {
        putenv('LARAOWL_QUEUE_CONNECTION=redis');
        putenv('LARAOWL_QUEUE=custom-queue');
        $this->rebindConfig();

        $ingest = $this->makeIngest();
        $ingest->write(['type' => 'test']);
        $ingest->digest();

        $this->assertCount(1, $this->dispatchedJobs);
        $job = $this->dispatchedJobs[0];
        $this->assertInstanceOf(TransmitRecords::class, $job);
        $this->assertSame('redis', $job->connection);
        $this->assertSame('custom-queue', $job->queue);
    }

    public function test_it_does_not_dispatch_a_job_by_default_and_falls_back_to_synchronous_transmit(): void
    {
        // No LARAOWL_QUEUE_CONNECTION set: this is the out-of-the-box config,
        // which must keep sending synchronously.
        $this->rebindConfig();

        $ingest = $this->makeIngest();
        $ingest->write(['type' => 'test']);
        $ingest->digest();

        $this->assertCount(0, $this->dispatchedJobs);
    }

    public function test_it_uses_the_connections_default_queue_when_no_queue_name_is_configured(): void
    {
        // Setting only LARAOWL_QUEUE_CONNECTION -- the documented one-variable
        // setup -- must push to the connection's default queue, the one a
        // stock `queue:work <connection>` consumes. A non-empty default here
        // would silently strand every batch on a queue nobody listens on.
        putenv('LARAOWL_QUEUE_CONNECTION=redis');
        $this->rebindConfig();

        $ingest = $this->makeIngest();
        $ingest->write(['type' => 'test']);
        $ingest->digest();

        $this->assertCount(1, $this->dispatchedJobs);
        $this->assertNull($this->dispatchedJobs[0]->queue);
    }

    public function test_an_empty_queue_name_falls_back_to_the_connections_default_queue(): void
    {
        // LARAOWL_QUEUE='' must behave like "not configured" too, not push
        // jobs onto a queue literally named ''.
        putenv('LARAOWL_QUEUE_CONNECTION=redis');
        putenv('LARAOWL_QUEUE=');
        $this->rebindConfig();

        $ingest = $this->makeIngest();
        $ingest->write(['type' => 'test']);
        $ingest->digest();

        $this->assertCount(1, $this->dispatchedJobs);
        $this->assertNull($this->dispatchedJobs[0]->queue);
    }

    public function test_it_delays_the_job_when_a_queue_delay_is_configured(): void
    {
        putenv('LARAOWL_QUEUE_CONNECTION=redis');
        $this->rebindConfig();
        Container::getInstance()->make('config')->set('laraowl.queue.delay', 30);

        $ingest = $this->makeIngest();
        $ingest->write(['type' => 'test']);
        $ingest->digest();

        $this->assertCount(1, $this->dispatchedJobs);
        $this->assertNotNull($this->dispatchedJobs[0]->delay);
    }

    public function test_it_resolves_lazy_values_so_the_job_survives_serialization(): void
    {
        // A LazyValue wraps a Closure, and queued jobs are serialized with
        // serialize(), not json_encode(). Without the JSON round-trip in
        // digest() this blows up with "Serialization of 'Closure' is not
        // allowed" the moment the driver pushes the payload.
        putenv('LARAOWL_QUEUE_CONNECTION=redis');
        $this->rebindConfig();

        $ingest = $this->makeIngest();
        $ingest->write([
            't' => 'request',
            'user' => new LazyValue(fn () => ['id' => 'user-1']),
        ]);
        $ingest->digest();

        $this->assertCount(1, $this->dispatchedJobs);
        $job = $this->dispatchedJobs[0];

        $records = new ReflectionProperty(TransmitRecords::class, 'records');
        $this->assertSame(
            [['t' => 'request', 'user' => ['id' => 'user-1']]],
            $records->getValue($job),
        );

        // The assertion that actually reproduces the bug: no Closure left.
        $this->assertIsString(serialize($job));
    }

    public function test_it_marks_the_queued_payload_as_unsampled(): void
    {
        // Core::prepareForJob() restores sampling from the payload context,
        // and getSamplingFromContext() defaults to TRUE when the key is
        // absent -- so an unmarked transmit job runs sampled, and on a
        // database connection the driver's own SQL then feeds the next
        // digest(), dispatching another transmit job forever.
        putenv('LARAOWL_QUEUE_CONNECTION=database');
        $this->rebindConfig();

        $ingest = $this->makeIngest();
        $ingest->write(['type' => 'test']);
        $ingest->digest();

        $this->assertSame([false], $this->samplingDuringDispatch);
    }

    public function test_it_restores_the_surrounding_sampling_context_after_dispatching(): void
    {
        putenv('LARAOWL_QUEUE_CONNECTION=database');
        $this->rebindConfig();
        Compatibility::addSamplingToContext(true);

        $ingest = $this->makeIngest();
        $ingest->write(['type' => 'test']);
        $ingest->digest();

        $this->assertTrue(Compatibility::getSamplingFromContext(null));
    }

    public function test_it_logs_and_contains_a_queue_backend_failure(): void
    {
        // digest()'s only catching caller hands the exception to
        // LaraowlClient::unrecoverableExceptionOccurred(), a no-op unless the
        // app registered a handler -- so an unguarded throw here drops the
        // batch with nothing in any log. digest() is also reachable
        // mid-request from write() when the buffer fills up.
        putenv('LARAOWL_QUEUE_CONNECTION=nope');
        $this->rebindConfig();
        $this->bindDispatcher(function () {
            throw new RuntimeException('Queue connection [nope] is not configured.');
        });

        $ingest = $this->makeIngest();
        $ingest->write(['type' => 'test']);
        $ingest->digest();

        $this->assertCount(1, $this->loggedErrors);
        $this->assertStringContainsString('failed to queue records', $this->loggedErrors[0]);
        $this->assertStringContainsString('Queue connection [nope] is not configured.', $this->loggedErrors[0]);
    }

    private function bindDispatcher(callable $onDispatch): void
    {
        // Only dispatch() is ever exercised by digest(); a stub leaves every
        // other Dispatcher method as a harmless no-op.
        $dispatcher = $this->createStub(Dispatcher::class);
        $dispatcher->method('dispatch')->willReturnCallback($onDispatch);

        Container::getInstance()->instance(Dispatcher::class, $dispatcher);
    }

    private function rebindConfig(): void
    {
        Container::getInstance()->instance('config', new Repository([
            'laraowl' => require __DIR__.'/../../config/laraowl.php',
        ]));
    }

    private function makeIngest(): HttpIngest
    {
        // Unroutable loopback port: the synchronous fallback path fails fast
        // (connection refused) instead of hitting the network or timing out.
        return new HttpIngest(
            endpoint: 'http://127.0.0.1:1',
            token: 'test-token',
            timeout: 0.5,
            buffer: new RecordsBuffer(10),
        );
    }
}
