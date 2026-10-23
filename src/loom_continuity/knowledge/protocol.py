"""Storage-independent durable-knowledge contract.

`scope` isolates an independent knowledge domain. It is not a chat session.
It may name a workflow, tenant, project, incident, job, or agent run.

`LookupStatus.VALID` means provenance dependencies are current.
It does not mean the stored value is semantically true.
"""

from typing import Any, Protocol, Sequence, runtime_checkable

from loom_continuity.knowledge.types import FactRevision, Lookup, ProvenanceRef


@runtime_checkable
class KnowledgeStore(Protocol):
    def put_fact(
        self,
        scope: str,
        key: str,
        value: Any,
        source_step: str,
        provenance: Sequence[ProvenanceRef],
        status: str = "established",
    ) -> int: ...

    def get_fact(self, scope: str, key: str) -> FactRevision | None: ...

    def get_revisions(self, scope: str, key: str) -> list[FactRevision]: ...

    def get_current_version(self, scope: str, key: str) -> int | None: ...

    def get_valid_fact(self, scope: str, key: str) -> Lookup: ...

    def get_current_facts(self, scope: str) -> list[FactRevision]: ...
