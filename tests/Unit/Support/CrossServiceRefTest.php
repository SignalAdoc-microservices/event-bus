<?php

declare(strict_types=1);

namespace Signaladoc\EventBus\Tests\Unit\Support;

use InvalidArgumentException;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\Attributes\Test;
use PHPUnit\Framework\TestCase;
use Signaladoc\EventBus\Support\CrossServiceRef;

/**
 * @see ../../../src/Support/CrossServiceRef.php
 * @see microservice/ARCHITECTURE.md §12.1
 */
final class CrossServiceRefTest extends TestCase
{
    #[Test]
    public function format_builds_a_ref_string(): void
    {
        $this->assertSame(
            'billing-service.plans:42',
            CrossServiceRef::format('billing-service', 'plans', 42),
        );
    }

    #[Test]
    public function format_accepts_string_ids(): void
    {
        $this->assertSame(
            'chat-backend.whats_app_users:abc-123',
            CrossServiceRef::format('chat-backend', 'whats_app_users', 'abc-123'),
        );
    }

    #[Test]
    public function format_rejects_empty_parts(): void
    {
        $this->expectException(InvalidArgumentException::class);
        CrossServiceRef::format('', 'plans', 42);
    }

    #[Test]
    public function parse_round_trips_a_well_formed_ref(): void
    {
        $parsed = CrossServiceRef::parse('billing-service.plans:42');

        $this->assertSame('billing-service', $parsed['service']);
        $this->assertSame('plans', $parsed['table']);
        $this->assertSame('42', $parsed['id']);
    }

    #[Test]
    public function parse_handles_dotted_service_names(): void
    {
        $parsed = CrossServiceRef::parse('partner-service.partners:99');

        $this->assertSame('partner-service', $parsed['service']);
        $this->assertSame('partners', $parsed['table']);
        $this->assertSame('99', $parsed['id']);
    }

    #[Test]
    public function parse_handles_uuid_style_ids(): void
    {
        $parsed = CrossServiceRef::parse('chat-backend.whats_app_users:abc-123');

        $this->assertSame('chat-backend', $parsed['service']);
        $this->assertSame('whats_app_users', $parsed['table']);
        $this->assertSame('abc-123', $parsed['id']);
    }

    /**
     * @return array<string, array{string}>
     */
    public static function malformedRefs(): array
    {
        return [
            'no colon'              => ['billing-service.plans42'],
            'no dot'                => ['billing-service:42'],
            'empty service'         => ['.plans:42'],
            'empty table'           => ['billing-service.:42'],
            'empty id'              => ['billing-service.plans:'],
            'invalid characters'    => ['billing service.plans:42'],
            'leading whitespace'    => [' billing-service.plans:42'],
            'trailing whitespace'   => ['billing-service.plans:42 '],
        ];
    }

    #[Test]
    #[DataProvider('malformedRefs')]
    public function parse_rejects_malformed_strings(string $ref): void
    {
        $this->expectException(InvalidArgumentException::class);
        CrossServiceRef::parse($ref);
    }

    #[Test]
    public function id_for_returns_the_id_when_service_and_table_match(): void
    {
        $this->assertSame(
            '42',
            CrossServiceRef::idFor('billing-service.plans:42', 'billing-service', 'plans'),
        );
    }

    #[Test]
    public function id_for_rejects_mismatched_service(): void
    {
        $this->expectException(InvalidArgumentException::class);
        CrossServiceRef::idFor('insurance-service.plans:42', 'billing-service', 'plans');
    }

    #[Test]
    public function id_for_rejects_mismatched_table(): void
    {
        $this->expectException(InvalidArgumentException::class);
        CrossServiceRef::idFor('billing-service.subscriptions:42', 'billing-service', 'plans');
    }

    #[Test]
    public function is_valid_distinguishes_well_formed_from_malformed(): void
    {
        $this->assertTrue(CrossServiceRef::isValid('billing-service.plans:42'));
        $this->assertFalse(CrossServiceRef::isValid('not-a-ref'));
    }
}
