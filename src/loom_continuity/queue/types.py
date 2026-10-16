"""Storage-independent work-queue values.

An event is a fact with references ("document.uploaded" + where the
document is), not the payload itself: large data stays in blob or row
storage and the event points at it."""

from enum import StrEnum
from typing import Any, TypedDict


class EventStatus(StrEnum):
    PENDING = "pending"
    PROCESSING = "processing"
    DONE = "done"
    DEAD = "dead"
    LEASE_LOST = "lease_lost"  # returned by fail/ack with a claim token that is no longer current


class ClaimedEvent(TypedDict):
    event_id: str
    type: str
    schema_version: int
    payload: dict[str, Any]
    correlation_id: str | None
    attempt: int  # 1 on first delivery, 2+ on redelivery
    # Fencing token of this claim: increases on every claim of the event.
    # renew/ack/fail with it succeed only while this claim is the current
    # one -- a worker whose lease expired (and the event was claimed again)
    # cannot extend, complete or fail the newer claim.
    claim_token: int
