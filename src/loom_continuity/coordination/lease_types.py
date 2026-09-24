"""Storage-independent repair-lease values.

`claim` reports a busy lease as `ClaimStatus.DENIED`. It does not raise.
"""

from dataclasses import dataclass
from enum import StrEnum


@dataclass(frozen=True)
class Lease:
    scope: str
    key: str
    owner_id: str
    expected_versions: dict[str, int]
    lease_until: str


class ClaimStatus(StrEnum):
    ACQUIRED = "ACQUIRED"
    DENIED = "DENIED"


@dataclass(frozen=True)
class ClaimResult:
    status: ClaimStatus
    lease: Lease | None = None
