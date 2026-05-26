<?php

declare(strict_types=1);

namespace Signaladoc\EventBus\Tests\Unit\Support;

use Illuminate\Redis\Connections\Connection;
use PHPUnit\Framework\Attributes\Test;
use PHPUnit\Framework\TestCase;
use RuntimeException;
use Signaladoc\EventBus\Support\RawRedis;
use stdClass;

/**
 * Guards the raw-RESP dispatch that PhpRedisStreamPublisher /
 * PhpRedisStreamReader rely on. Regression target: phpredis's typed
 * `Redis::xadd($key, $id, $values, $maxlen, $approximate)` rejects raw
 * positional args ("expects at most 6 arguments, 7 given"). This test
 * proves RawRedis bypasses the typed wrapper.
 *
 * @see ../../../src/Support/RawRedis.php
 */
final class RawRedisTest extends TestCase
{
    #[Test]
    public function dispatches_via_rawCommand_when_client_supports_phpredis_api(): void
    {
        $client = new class
        {
            public ?string $command = null;

            /** @var list<mixed> */
            public array $args = [];

            public function rawCommand(string $command, mixed ...$args): string
            {
                $this->command = $command;
                $this->args = $args;

                return '1717-0';
            }
        };

        $conn = $this->makeConnectionReturning($client);

        $result = RawRedis::send($conn, 'XADD', [
            'billing.cycle', 'MAXLEN', '~', '1000', '*', 'envelope', '{"x":1}',
        ]);

        $this->assertSame('1717-0', $result);
        $this->assertSame('XADD', $client->command);
        $this->assertSame(
            ['billing.cycle', 'MAXLEN', '~', '1000', '*', 'envelope', '{"x":1}'],
            $client->args,
        );
    }

    #[Test]
    public function dispatches_via_executeRaw_when_client_supports_predis_api(): void
    {
        $client = new class
        {
            /** @var list<mixed>|null */
            public ?array $payload = null;

            public function executeRaw(array $args): mixed
            {
                $this->payload = $args;

                return '1717-1';
            }
        };

        $conn = $this->makeConnectionReturning($client);

        $result = RawRedis::send($conn, 'XACK', ['billing.cycle', 'g1', '1717-0']);

        $this->assertSame('1717-1', $result);
        // Predis path packs command in front of args.
        $this->assertSame(['XACK', 'billing.cycle', 'g1', '1717-0'], $client->payload);
    }

    #[Test]
    public function prefers_phpredis_rawCommand_when_both_methods_exist(): void
    {
        // A misbehaving wrapper that exposes BOTH methods — we want rawCommand
        // to win because that's the phpredis-native entrypoint.
        $client = new class
        {
            public bool $rawCommandCalled = false;

            public bool $executeRawCalled = false;

            public function rawCommand(string $command, mixed ...$args): string
            {
                $this->rawCommandCalled = true;

                return 'ok';
            }

            public function executeRaw(array $args): mixed
            {
                $this->executeRawCalled = true;

                return 'wrong';
            }
        };

        RawRedis::send($this->makeConnectionReturning($client), 'XPENDING', ['s', 'g']);

        $this->assertTrue($client->rawCommandCalled);
        $this->assertFalse($client->executeRawCalled);
    }

    #[Test]
    public function throws_runtime_exception_when_client_supports_neither_api(): void
    {
        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('supports neither');

        RawRedis::send($this->makeConnectionReturning(new stdClass()), 'XADD', ['s']);
    }

    private function makeConnectionReturning(object $client): Connection
    {
        return new class($client) extends Connection
        {
            public function __construct(private readonly object $stubClient) {}

            public function client(): object
            {
                return $this->stubClient;
            }

            public function createSubscription($channels, \Closure $callback, $method = 'subscribe'): void {}
        };
    }
}
