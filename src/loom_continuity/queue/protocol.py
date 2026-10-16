"""Work-queue contract without persistence assumptions.

Delivery semantics every implementation must state and every consumer
must tolerate:

    at-least-once  a consumer that dies after `claim` gets the event
                   redelivered once its lease expires: consumers must be
                   idempotent (dedupe on event_id or on the payload's own key);
    no ordering    a retried event can overtake a newer one;
    retries        `fail` returns the event after backoff, until
                   max attempts, then it is parked as "dead" with its error;
                   `fail(..., retry=False)` parks a permanent failure at once;
    ownership      every claim gets a higher `claim_token` (fencing token);
                   `renew` (heartbeat), `ack` and `fail` given the token act
                   only while that claim is current -- a worker whose lease
                   expired cannot extend, complete or fail the newer claim.
"""

from typing import Protocol, runtime_checkable

from loom_continuity.queue.types import ClaimedEvent


@runtime_checkable
class WorkQueue(Protocol):
    def publish(self, type_: str, payload: dict, schema_version: int = 1,
                correlation_id: str | None = None) -> str: ...

    def claim(self, types: list[str]) -> ClaimedEvent | None: ...

    def renew(self, event_id: str, claim_token: int, lease_s: float | None = None) -> bool: ...

    def ack(self, event_id: str, claim_token: int | None = None) -> bool: ...

    def fail(self, event_id: str, error: str, retry: bool = True, claim_token: int | None = None) -> str: ...
