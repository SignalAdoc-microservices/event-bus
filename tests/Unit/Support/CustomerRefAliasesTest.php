<?php

declare(strict_types=1);

namespace Signaladoc\EventBus\Tests\Unit\Support;

use PHPUnit\Framework\Attributes\Test;
use PHPUnit\Framework\TestCase;
use Signaladoc\EventBus\Support\CustomerRefAliases;

final class CustomerRefAliasesTest extends TestCase
{
    #[Test]
    public function candidates_empty_for_null_or_blank(): void
    {
        $this->assertSame([], CustomerRefAliases::candidates(null));
        $this->assertSame([], CustomerRefAliases::candidates(''));
        $this->assertSame([], CustomerRefAliases::candidates('   '));
    }

    #[Test]
    public function candidates_bridges_telemedicine_backend_to_short_form(): void
    {
        $this->assertSame(
            [
                'telemedicine-backend.users:42',
                'telemedicine.users:42',
            ],
            CustomerRefAliases::candidates('telemedicine-backend.users:42'),
        );
    }

    #[Test]
    public function candidates_bridges_short_form_to_telemedicine_backend(): void
    {
        $this->assertSame(
            [
                'telemedicine.users:42',
                'telemedicine-backend.users:42',
            ],
            CustomerRefAliases::candidates('telemedicine.users:42'),
        );
    }

    #[Test]
    public function candidates_leaves_other_refs_unchanged(): void
    {
        $this->assertSame(
            ['chat-backend.whats_app_users:abc'],
            CustomerRefAliases::candidates('chat-backend.whats_app_users:abc'),
        );
    }
}
