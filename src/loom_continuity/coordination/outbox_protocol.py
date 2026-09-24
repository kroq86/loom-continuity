"""Outbox and delivery contracts without persistence assumptions."""

from typing import Callable, Protocol, runtime_checkable

from loom_continuity.coordination.outbox_types import OutboxEvent


@runtime_checkable
class Outbox(Protocol):
    def pending(self, scope: str | None = None) -> list[OutboxEvent]: ...

    def mark_delivered(self, event_id: str) -> None: ...

    def get(self, event_id: str) -> OutboxEvent | None: ...


def dispatch_pending(
    outbox: Outbox,
    consumer: Callable[[OutboxEvent], None],
    scope: str | None = None,
) -> list[str]:
    delivered = []
    for event in outbox.pending(scope=scope):
        consumer(event)
        outbox.mark_delivered(event.event_id)
        delivered.append(event.event_id)
    return delivered
