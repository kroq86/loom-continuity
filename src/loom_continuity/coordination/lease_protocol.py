"""Storage-independent repair-lease contract."""

from typing import Protocol, runtime_checkable

from loom_continuity.coordination.lease_types import ClaimResult, Lease


@runtime_checkable
class RepairLeaseStore(Protocol):
    def claim(
        self,
        scope: str,
        key: str,
        owner_id: str,
        expected_versions: dict[str, int],
        lease_seconds: float = 30.0,
    ) -> ClaimResult: ...

    def get_current(self, scope: str, key: str) -> Lease | None: ...

    def release(self, scope: str, key: str, owner_id: str) -> None: ...
