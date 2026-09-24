"""SQLite outbox adapter and atomic SQLite memory/outbox commit."""

import sqlite3
import uuid
from contextlib import closing
from datetime import datetime, timezone
from typing import Any, Sequence

from loom_continuity.coordination.outbox_types import OutboxEvent, OutboxStatus
from loom_continuity.knowledge.sqlite import SqliteKnowledgeStore
from loom_continuity.knowledge.types import ProvenanceRef

EVENT_TYPE_FACT_REVISION_CREATED = "FactRevisionCreated"


class SqliteOutbox:
    def __init__(self, db_path: str):
        self.db_path = db_path
        with closing(sqlite3.connect(self.db_path)) as conn:
            conn.execute(
                """CREATE TABLE IF NOT EXISTS outbox (
                    event_id TEXT PRIMARY KEY,
                    scope TEXT NOT NULL,
                    key TEXT NOT NULL,
                    version INTEGER NOT NULL,
                    previous_version INTEGER,
                    event_type TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    status TEXT NOT NULL
                )"""
            )
            conn.commit()

    def enqueue(
        self,
        conn: sqlite3.Connection,
        scope: str,
        key: str,
        version: int,
        previous_version: int | None,
        event_type: str = EVENT_TYPE_FACT_REVISION_CREATED,
    ) -> str:
        event_id = str(uuid.uuid4())
        conn.execute(
            "INSERT INTO outbox (event_id, scope, key, version, previous_version, event_type, created_at, status) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, 'pending')",
            (
                event_id,
                scope,
                key,
                version,
                previous_version,
                event_type,
                datetime.now(timezone.utc).isoformat(),
            ),
        )
        return event_id

    def pending(self, scope: str | None = None) -> list[OutboxEvent]:
        with closing(sqlite3.connect(self.db_path)) as conn:
            if scope is None:
                rows = conn.execute(
                    "SELECT event_id, scope, key, version, previous_version, event_type, created_at, status "
                    "FROM outbox WHERE status = 'pending' ORDER BY created_at"
                ).fetchall()
            else:
                rows = conn.execute(
                    "SELECT event_id, scope, key, version, previous_version, event_type, created_at, status "
                    "FROM outbox WHERE status = 'pending' AND scope = ? ORDER BY created_at",
                    (scope,),
                ).fetchall()
        return [self._row_to_event(row) for row in rows]

    def mark_delivered(self, event_id: str) -> None:
        with closing(sqlite3.connect(self.db_path)) as conn:
            conn.execute("UPDATE outbox SET status = 'delivered' WHERE event_id = ?", (event_id,))
            conn.commit()

    def get(self, event_id: str) -> OutboxEvent | None:
        with closing(sqlite3.connect(self.db_path)) as conn:
            row = conn.execute(
                "SELECT event_id, scope, key, version, previous_version, event_type, created_at, status "
                "FROM outbox WHERE event_id = ?",
                (event_id,),
            ).fetchone()
        return None if row is None else self._row_to_event(row)

    @staticmethod
    def _row_to_event(row) -> OutboxEvent:
        return OutboxEvent(*row[:-1], status=OutboxStatus(row[-1]))


def commit_fact_with_event(
    memory: SqliteKnowledgeStore,
    outbox: SqliteOutbox,
    scope: str,
    key: str,
    value: Any,
    source_step: str,
    provenance: Sequence[ProvenanceRef],
    event_type: str = EVENT_TYPE_FACT_REVISION_CREATED,
) -> tuple[int, str]:
    _require_shared_database(memory, outbox)
    previous_version = memory.get_current_version(scope, key)
    conn = sqlite3.connect(memory.db_path)
    try:
        version = memory.put_fact(scope, key, value, source_step, provenance, _conn=conn)
        event_id = outbox.enqueue(conn, scope, key, version, previous_version, event_type)
        conn.commit()
    finally:
        conn.close()
    return version, event_id


def commit_fact_with_event_if_versions_match(
    memory: SqliteKnowledgeStore,
    outbox: SqliteOutbox,
    scope: str,
    key: str,
    value: Any,
    source_step: str,
    provenance: Sequence[ProvenanceRef],
    expected_versions: dict[str, int],
    event_type: str = EVENT_TYPE_FACT_REVISION_CREATED,
) -> tuple[int, str] | None:
    _require_shared_database(memory, outbox)
    conn = sqlite3.connect(memory.db_path, isolation_level=None, timeout=10)
    try:
        conn.execute("BEGIN IMMEDIATE")
        for dependency_key, expected_version in expected_versions.items():
            row = conn.execute(
                "SELECT MAX(version) FROM facts WHERE scope = ? AND key = ?",
                (scope, dependency_key),
            ).fetchone()
            current_version = row[0] if row and row[0] is not None else None
            if current_version != expected_version:
                conn.execute("ROLLBACK")
                return None
        previous_row = conn.execute(
            "SELECT MAX(version) FROM facts WHERE scope = ? AND key = ?",
            (scope, key),
        ).fetchone()
        previous_version = previous_row[0] if previous_row and previous_row[0] is not None else None
        version = memory.put_fact(scope, key, value, source_step, provenance, _conn=conn)
        event_id = outbox.enqueue(conn, scope, key, version, previous_version, event_type)
        conn.execute("COMMIT")
    finally:
        conn.close()
    return version, event_id


def _require_shared_database(memory: SqliteKnowledgeStore, outbox: SqliteOutbox) -> None:
    if memory.db_path != outbox.db_path:
        raise ValueError("SQLite memory and outbox must share the same database file")
