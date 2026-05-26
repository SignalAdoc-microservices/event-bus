<?php

namespace Signaladoc\EventBus\Support;

use Illuminate\Redis\Connections\Connection;
use RuntimeException;

/**
 * Send a Redis command through the raw RESP entrypoint of whichever
 * client Laravel is wired to (phpredis or Predis).
 *
 * Why this exists: `Illuminate\Redis\Connections\Connection::command()`
 * dispatches to the underlying client's *typed* method — e.g. phpredis's
 * `Redis::xadd($key, $id, $values, $maxlen = 0, $approximate = false)`
 * — which has a fixed signature that DOES NOT accept raw RESP-style
 * positional args like `[$stream, 'MAXLEN', '~', $n, '*', 'field', $val]`.
 * Passing N>5 args triggers a hard error like:
 *   "Redis::xadd() expects at most 6 arguments, 7 given"
 *
 * For Streams commands (XADD/XREADGROUP/XCLAIM/XPENDING) we want raw
 * positional args because (a) the typed wrappers differ across phpredis
 * versions, especially for XCLAIM/XPENDING IDLE; (b) Predis exposes
 * them via a separate `executeRaw` API entirely. This helper makes both
 * clients accept the same shape.
 *
 * phpredis:  Redis::rawCommand(string $command, mixed ...$args): mixed
 * Predis:    Predis\Client::executeRaw(array $args): mixed
 *
 * @see microservice/ARCHITECTURE.md §13 (Inter-service events)
 */
final class RawRedis
{
    /**
     * @param  list<string|int>  $args  RESP positional args, excluding the command itself.
     */
    public static function send(Connection $conn, string $command, array $args): mixed
    {
        $client = $conn->client();

        // phpredis (\Redis or \RedisCluster).
        if (method_exists($client, 'rawCommand')) {
            return $client->rawCommand($command, ...$args);
        }

        // Predis (Predis\ClientInterface).
        if (method_exists($client, 'executeRaw')) {
            return $client->executeRaw([$command, ...$args]);
        }

        throw new RuntimeException(sprintf(
            "Redis client %s supports neither phpredis's rawCommand() nor Predis's executeRaw(); cannot send raw '%s'.",
            $client::class,
            $command,
        ));
    }
}
