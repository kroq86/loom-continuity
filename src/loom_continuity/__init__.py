"""loom_continuity maintains durable, versioned knowledge whose reusability is
determined by current provenance, and coordinates safe repair and propagation
across process failure.

VALID means provenance dependencies are current, not that a value is true.
Outbox events notify that authoritative state changed; they do not carry it.
"""
from loom_continuity.backends.base import LLMBackend, StatefulLLMBackend
from loom_continuity.coordination.lease_protocol import RepairLeaseStore
from loom_continuity.coordination.lease_types import ClaimResult, ClaimStatus, Lease
from loom_continuity.coordination.outbox_protocol import Outbox, dispatch_pending
from loom_continuity.coordination.outbox_types import OutboxEvent, OutboxStatus
from loom_continuity.coordination.sqlite_lease import SqliteRepairLease
from loom_continuity.coordination.sqlite_outbox import (
    SqliteOutbox,
    commit_fact_with_event,
    commit_fact_with_event_if_versions_match,
)
from loom_continuity.knowledge.protocol import KnowledgeStore
from loom_continuity.knowledge.sqlite import SqliteKnowledgeStore
from loom_continuity.knowledge.types import FactRevision, Lookup, LookupStatus, ProvenanceRef

__all__ = [
    "LLMBackend",
    "StatefulLLMBackend",
    "FactRevision",
    "ProvenanceRef",
    "Lookup",
    "LookupStatus",
    "KnowledgeStore",
    "SqliteKnowledgeStore",
    "OutboxEvent",
    "OutboxStatus",
    "Outbox",
    "SqliteOutbox",
    "commit_fact_with_event",
    "commit_fact_with_event_if_versions_match",
    "dispatch_pending",
    "Lease",
    "ClaimStatus",
    "ClaimResult",
    "RepairLeaseStore",
    "SqliteRepairLease",
]
