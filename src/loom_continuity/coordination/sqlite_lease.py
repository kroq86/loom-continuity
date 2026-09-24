"""SQLite adapter for durable per-key repair leases."""

import json
import sqlite3
from contextlib import closing
from datetime import datetime, timedelta, timezone

from loom_continuity.coordination.lease_types import ClaimResult, ClaimStatus, Lease


class SqliteRepairLease:
    def __init__(self, db_path: str):
        self.db_path = db_path
        self._init_schema()

    def claim(
        self,
        scope: str,
        key: str,
        owner_id: str,
        expected_versions: dict[str, int],
        lease_seconds: float = 30.0,
    ) -> ClaimResult:
        now = datetime.now(timezone.utc)
        now_iso = now.isoformat()
        lease_until = (now + timedelta(seconds=lease_seconds)).isoformat()
        conn = sqlite3.connect(self.db_path, isolation_level=None, timeout=10)
        try:
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute(
                "SELECT owner_id, lease_until FROM repair_leases WHERE scope = ? AND key = ?",
                (scope, key),
            ).fetchone()
            if row is not None:
                existing_owner, existing_until = row
                if existing_until > now_iso and existing_owner != owner_id:
                    conn.execute("ROLLBACK")
                    return ClaimResult(status=ClaimStatus.DENIED)
            conn.execute(
                "INSERT INTO repair_leases (scope, key, owner_id, expected_versions, lease_until) "
                "VALUES (?, ?, ?, ?, ?) "
                "ON CONFLICT(scope, key) DO UPDATE SET owner_id = excluded.owner_id, "
                "expected_versions = excluded.expected_versions, lease_until = excluded.lease_until",
                (scope, key, owner_id, json.dumps(expected_versions), lease_until),
            )
            conn.execute("COMMIT")
        finally:
            conn.close()
        lease = Lease(scope, key, owner_id, expected_versions, lease_until)
        return ClaimResult(status=ClaimStatus.ACQUIRED, lease=lease)

    def get_current(self, scope: str, key: str) -> Lease | None:
        with closing(sqlite3.connect(self.db_path)) as conn:
            row = conn.execute(
                "SELECT owner_id, expected_versions, lease_until FROM repair_leases "
                "WHERE scope = ? AND key = ?",
                (scope, key),
            ).fetchone()
        if row is None:
            return None
        owner_id, expected_versions, lease_until = row
        return Lease(scope, key, owner_id, json.loads(expected_versions), lease_until)

    def release(self, scope: str, key: str, owner_id: str) -> None:
        with closing(sqlite3.connect(self.db_path)) as conn:
            conn.execute(
                "DELETE FROM repair_leases WHERE scope = ? AND key = ? AND owner_id = ?",
                (scope, key, owner_id),
            )
            conn.commit()

    def _init_schema(self) -> None:
        with closing(sqlite3.connect(self.db_path)) as conn:
            conn.execute(
                """CREATE TABLE IF NOT EXISTS repair_leases (
                    scope TEXT NOT NULL,
                    key TEXT NOT NULL,
                    owner_id TEXT NOT NULL,
                    expected_versions TEXT NOT NULL,
                    lease_until TEXT NOT NULL,
                    PRIMARY KEY (scope, key)
                )"""
            )
            conn.commit()
