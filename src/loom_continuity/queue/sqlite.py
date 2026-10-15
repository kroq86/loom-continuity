"""SQLite adapter for a durable work queue with leases, retries with
exponential backoff, and a dead-letter state.

Several worker processes may share one database file: `claim` is a
single BEGIN IMMEDIATE transaction, so one event goes to one worker at a
time; the lease lets another worker take it over if that one dies."""

import json
import sqlite3
import time
import uuid
from contextlib import closing
from datetime import datetime, timezone

from loom_continuity.queue.types import ClaimedEvent, EventStatus


class SqliteEventQueue:
    def __init__(self, db_path: str, max_attempts: int = 3, lease_s: float = 60.0, base_backoff_s: float = 1.0):
        if max_attempts < 1:
            raise ValueError("max_attempts must be at least 1")
        self.db_path, self.max_attempts, self.lease_s, self.base_backoff_s = (
            db_path, max_attempts, lease_s, base_backoff_s)
        with closing(self._connect()) as conn, conn:
            conn.execute("""CREATE TABLE IF NOT EXISTS events (
                event_id TEXT PRIMARY KEY, type TEXT NOT NULL, schema_version INTEGER NOT NULL,
                payload TEXT NOT NULL, correlation_id TEXT, created_at TEXT NOT NULL,
                status TEXT NOT NULL, attempts INTEGER NOT NULL DEFAULT 0,
                available_at REAL NOT NULL, lease_until REAL, last_error TEXT)""")

    def _connect(self) -> sqlite3.Connection:
        return sqlite3.connect(self.db_path, timeout=10)

    def publish(self, type_: str, payload: dict, schema_version: int = 1, correlation_id: str | None = None) -> str:
        event_id = str(uuid.uuid4())
        with closing(self._connect()) as conn, conn:
            conn.execute("INSERT INTO events (event_id, type, schema_version, payload, correlation_id, created_at, "
                         "status, available_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                         (event_id, type_, schema_version, json.dumps(payload), correlation_id,
                          datetime.now(timezone.utc).isoformat(), EventStatus.PENDING, time.time()))
        return event_id

    def claim(self, types: list[str]) -> ClaimedEvent | None:
        """Lease one available event of the given types: pending and due,
        or processing with an expired lease (its consumer died). None if
        nothing is ready."""
        now = time.time()
        marks = ",".join("?" * len(types))
        with closing(self._connect()) as conn:
            conn.isolation_level = None
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute(
                f"SELECT event_id, type, schema_version, payload, correlation_id, attempts FROM events "
                f"WHERE type IN ({marks}) AND ((status = 'pending' AND available_at <= ?) "
                f"OR (status = 'processing' AND lease_until < ?)) ORDER BY available_at LIMIT 1",
                (*types, now, now)).fetchone()
            if row is None:
                conn.execute("COMMIT")
                return None
            conn.execute("UPDATE events SET status = 'processing', attempts = attempts + 1, lease_until = ? "
                         "WHERE event_id = ?", (now + self.lease_s, row[0]))
            conn.execute("COMMIT")
        return ClaimedEvent(event_id=row[0], type=row[1], schema_version=row[2], payload=json.loads(row[3]),
                            correlation_id=row[4], attempt=row[5] + 1)

    def ack(self, event_id: str) -> None:
        with closing(self._connect()) as conn, conn:
            conn.execute("UPDATE events SET status = 'done', lease_until = NULL WHERE event_id = ?", (event_id,))

    def fail(self, event_id: str, error: str, retry: bool = True) -> str:
        """Back to "pending" after backoff (base * 2^(attempt-1)), or
        "dead" once max_attempts are spent. Returns the new status.

        retry=False: the failure is permanent (a corrupt file, an event
        schema the consumer does not know) -- the same input fails the same
        way, so the event goes to the dead-letter state at once instead of
        spending the remaining attempts."""
        with closing(self._connect()) as conn, conn:
            attempts = conn.execute("SELECT attempts FROM events WHERE event_id = ?", (event_id,)).fetchone()[0]
            if attempts >= self.max_attempts or not retry:
                conn.execute("UPDATE events SET status = 'dead', last_error = ?, lease_until = NULL "
                             "WHERE event_id = ?", (error, event_id))
                return EventStatus.DEAD
            conn.execute("UPDATE events SET status = 'pending', last_error = ?, lease_until = NULL, "
                         "available_at = ? WHERE event_id = ?",
                         (error, time.time() + self.base_backoff_s * 2 ** (attempts - 1), event_id))
            return EventStatus.PENDING

    def list(self, status: str | None = None, limit: int = 50, after: str | None = None) -> list[dict]:
        """Events oldest first, for inspection (e.g. the dead-letter list).
        Keyset pagination: `after` is "<created_at>|<event_id>" of the
        last row seen."""
        where, args = [], []
        if status:
            where.append("status = ?")
            args.append(status)
        if after:
            ts, eid = after.split("|", 1)
            where.append("(created_at, event_id) > (?, ?)")
            args += [ts, eid]
        with closing(self._connect()) as conn:
            conn.row_factory = sqlite3.Row
            q = ("SELECT event_id, type, status, attempts, last_error, correlation_id, payload, created_at FROM events"
                 + (" WHERE " + " AND ".join(where) if where else "") + " ORDER BY created_at, event_id LIMIT ?")
            rows = conn.execute(q, (*args, limit)).fetchall()
        return [dict(r) for r in rows]
