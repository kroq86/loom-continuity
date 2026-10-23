"""SQLite adapter for append-only, provenance-aware knowledge."""

import json
import sqlite3
from contextlib import closing
from datetime import datetime, timezone
from typing import Any, Sequence

from loom_continuity.knowledge.types import FactRevision, Lookup, LookupStatus, ProvenanceRef


class SqliteKnowledgeStore:
    def __init__(self, db_path: str):
        self.db_path = db_path
        with closing(sqlite3.connect(self.db_path)) as conn:
            conn.execute(
                """CREATE TABLE IF NOT EXISTS facts (
                    scope TEXT NOT NULL,
                    key TEXT NOT NULL,
                    version INTEGER NOT NULL,
                    value TEXT NOT NULL,
                    source_step TEXT NOT NULL,
                    provenance TEXT NOT NULL,
                    status TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    PRIMARY KEY (scope, key, version)
                )"""
            )
            conn.commit()

    def put_fact(
        self,
        scope: str,
        key: str,
        value: Any,
        source_step: str,
        provenance: Sequence[ProvenanceRef],
        status: str = "established",
        _conn: sqlite3.Connection | None = None,
    ) -> int:
        created_at = datetime.now(timezone.utc).isoformat()
        owns_conn = _conn is None
        conn = _conn if _conn is not None else sqlite3.connect(self.db_path)
        try:
            row = conn.execute(
                "SELECT MAX(version) FROM facts WHERE scope = ? AND key = ?",
                (scope, key),
            ).fetchone()
            version = (row[0] + 1) if row and row[0] is not None else 1
            conn.execute(
                "INSERT INTO facts (scope, key, version, value, source_step, provenance, status, created_at) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    scope,
                    key,
                    version,
                    json.dumps(value),
                    source_step,
                    json.dumps(list(provenance)),
                    status,
                    created_at,
                ),
            )
            if owns_conn:
                conn.commit()
        finally:
            if owns_conn:
                conn.close()
        return version

    def get_fact(self, scope: str, key: str) -> FactRevision | None:
        with closing(sqlite3.connect(self.db_path)) as conn:
            row = conn.execute(
                "SELECT key, value, source_step, provenance, status, created_at, version FROM facts "
                "WHERE scope = ? AND key = ? ORDER BY version DESC LIMIT 1",
                (scope, key),
            ).fetchone()
        return None if row is None else self._row_to_fact(row)

    def get_revisions(self, scope: str, key: str) -> list[FactRevision]:
        with closing(sqlite3.connect(self.db_path)) as conn:
            rows = conn.execute(
                "SELECT key, value, source_step, provenance, status, created_at, version FROM facts "
                "WHERE scope = ? AND key = ? ORDER BY version ASC",
                (scope, key),
            ).fetchall()
        return [self._row_to_fact(row) for row in rows]

    def get_current_facts(self, scope: str) -> list[FactRevision]:
        """The current revision of every key in `scope`, ordered by key."""
        with closing(sqlite3.connect(self.db_path)) as conn:
            rows = conn.execute(
                "SELECT key, value, source_step, provenance, status, created_at, version FROM facts f "
                "WHERE scope = ? AND version = (SELECT MAX(version) FROM facts WHERE scope = f.scope AND key = f.key) "
                "ORDER BY key",
                (scope,),
            ).fetchall()
        return [self._row_to_fact(row) for row in rows]

    def get_current_version(self, scope: str, key: str) -> int | None:
        with closing(sqlite3.connect(self.db_path)) as conn:
            row = conn.execute(
                "SELECT MAX(version) FROM facts WHERE scope = ? AND key = ?",
                (scope, key),
            ).fetchone()
        return row[0] if row and row[0] is not None else None

    def get_valid_fact(
        self,
        scope: str,
        key: str,
        _seen: frozenset[str] = frozenset(),
    ) -> Lookup:
        fact = self.get_fact(scope, key)
        if fact is None:
            return Lookup(status=LookupStatus.MISSING, fact=None)
        if key in _seen:
            return Lookup(status=LookupStatus.VALID, fact=fact)
        for dependency in fact.provenance:
            if not isinstance(dependency, dict) or "key" not in dependency or "version" not in dependency:
                continue
            dependency_key = dependency["key"]
            if self.get_current_version(scope, dependency_key) != dependency["version"]:
                return Lookup(status=LookupStatus.STALE, fact=fact)
            dependency_lookup = self.get_valid_fact(scope, dependency_key, _seen | {key})
            if dependency_lookup.status != LookupStatus.VALID:
                return Lookup(status=LookupStatus.STALE, fact=fact)
        return Lookup(status=LookupStatus.VALID, fact=fact)

    @staticmethod
    def _row_to_fact(row) -> FactRevision:
        key, value, source_step, provenance, status, created_at, version = row
        return FactRevision(
            key=key,
            value=json.loads(value),
            source_step=source_step,
            provenance=json.loads(provenance),
            status=status,
            created_at=created_at,
            version=version,
        )
