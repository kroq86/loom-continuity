"""Storage-independent outbox values.

Current-State Notification Semantics

Outbox events do not carry authoritative fact values.
A consumer reacts to the event by re-reading the current value from KnowledgeStore.
Therefore an older delayed notification does not restore an older value.

This is not event sourcing and does not preserve historical transition payload semantics.
`scope` on an event identifies the knowledge domain. `version` identifies which
revision was committed. Neither field is a copy of that revision's value.
"""

from dataclasses import dataclass
from enum import StrEnum


class OutboxStatus(StrEnum):
    PENDING = "pending"
    DELIVERED = "delivered"


@dataclass(frozen=True)
class OutboxEvent:
    event_id: str
    scope: str
    key: str
    version: int
    previous_version: int | None
    event_type: str
    created_at: str
    status: OutboxStatus
