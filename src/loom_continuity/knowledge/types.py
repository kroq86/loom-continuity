"""Stable value types for versioned durable knowledge.

`LookupStatus.VALID` means provenance dependencies are current.
It does not mean the stored value is semantically true.
"""

from dataclasses import dataclass
from enum import StrEnum
from typing import Any, TypedDict


class LookupStatus(StrEnum):
    MISSING = "MISSING"
    STALE = "STALE"
    VALID = "VALID"


class ProvenanceRef(TypedDict):
    key: str
    version: int


@dataclass(frozen=True)
class FactRevision:
    key: str
    value: Any
    source_step: str
    provenance: list[ProvenanceRef]
    status: str = "established"
    created_at: str | None = None
    version: int = 1


@dataclass(frozen=True)
class Lookup:
    status: LookupStatus
    fact: FactRevision | None
