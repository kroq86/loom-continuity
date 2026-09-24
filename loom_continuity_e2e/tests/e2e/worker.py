"""Subprocess stages. Each mode is a separate Python process."""
import json
import os
import sqlite3
import sys
from contextlib import closing

from loom_continuity import (
    SqliteKnowledgeStore,
    SqliteOutbox,
    SqliteRepairLease,
    commit_fact_with_event,
    commit_fact_with_event_if_versions_match,
)

SCOPE = "job-1"
EVIDENCE = "evidence/source"
DERIVED = "derived/answer"


def emit(payload: dict) -> None:
    print(json.dumps(payload))


def establish(db: str) -> None:
    knowledge = SqliteKnowledgeStore(db)
    SqliteOutbox(db)
    knowledge.put_fact(SCOPE, EVIDENCE, "blue", "establish", [])
    version, _event = commit_fact_with_event(
        knowledge, SqliteOutbox(db), SCOPE, DERIVED, "monitor", "derive",
        [{"key": EVIDENCE, "version": 1}],
    )
    fact = knowledge.get_fact(SCOPE, DERIVED)
    emit({
        "value": fact.value,
        "version": version,
        "provenance": fact.provenance,
        "status": knowledge.get_valid_fact(SCOPE, DERIVED).status,
    })


def read_only(db: str) -> None:
    knowledge = SqliteKnowledgeStore(db)
    fact = knowledge.get_fact(SCOPE, DERIVED)
    emit({
        "value": fact.value,
        "version": fact.version,
        "provenance": fact.provenance,
        "status": knowledge.get_valid_fact(SCOPE, DERIVED).status,
        "writes": 0,
    })


def claim_and_die(db: str) -> None:
    result = SqliteRepairLease(db).claim(SCOPE, DERIVED, "worker-a", {EVIDENCE: 1}, lease_seconds=0.8)
    if result.status.value != "ACQUIRED":
        os._exit(2)
    os._exit(0)


def commit_and_die(db: str) -> None:
    knowledge = SqliteKnowledgeStore(db)
    outbox = SqliteOutbox(db)
    knowledge.put_fact(SCOPE, EVIDENCE, "blue", "establish", [])
    commit_fact_with_event(
        knowledge, outbox, SCOPE, DERIVED, "monitor", "derive",
        [{"key": EVIDENCE, "version": 1}],
    )
    os._exit(1)


def die_before_commit(db: str) -> None:
    SqliteKnowledgeStore(db)
    SqliteOutbox(db)
    os._exit(1)


def effect_then_die(db: str) -> None:
    outbox = SqliteOutbox(db)
    pending = outbox.pending(SCOPE)
    if len(pending) != 1:
        os._exit(2)
    event = pending[0]
    with closing(sqlite3.connect(db)) as conn:
        conn.execute("CREATE TABLE IF NOT EXISTS effects (event_id TEXT PRIMARY KEY)")
        conn.execute("INSERT OR IGNORE INTO effects (event_id) VALUES (?)", (event.event_id,))
        conn.commit()
    os._exit(1)


def continuity_process_1(db: str) -> None:
    knowledge = SqliteKnowledgeStore(db)
    outbox = SqliteOutbox(db)
    leases = SqliteRepairLease(db)
    knowledge.put_fact(SCOPE, EVIDENCE, "blue", "establish", [])
    commit_fact_with_event(
        knowledge, outbox, SCOPE, DERIVED, "monitor", "derive",
        [{"key": EVIDENCE, "version": 1}],
    )
    knowledge.put_fact(SCOPE, EVIDENCE, "red", "mutate", [])
    held = leases.claim(SCOPE, DERIVED, "worker-a", {EVIDENCE: 2}, lease_seconds=30)
    denied = leases.claim(SCOPE, DERIVED, "worker-b", {EVIDENCE: 2}, lease_seconds=30)
    if held.status.value != "ACQUIRED" or denied.status.value != "DENIED":
        os._exit(2)
    knowledge.put_fact(SCOPE, EVIDENCE, "green", "mutate-during-repair", [])
    rejected = commit_fact_with_event_if_versions_match(
        knowledge, outbox, SCOPE, DERIVED, "from-v2", "stale-repair",
        [{"key": EVIDENCE, "version": 2}],
        {EVIDENCE: 2},
    )
    if rejected is not None:
        os._exit(3)
    leases.release(SCOPE, DERIVED, "worker-a")
    acquired = leases.claim(SCOPE, DERIVED, "worker-b", {EVIDENCE: 3}, lease_seconds=30)
    committed = commit_fact_with_event_if_versions_match(
        knowledge, outbox, SCOPE, DERIVED, "escalate", "repair",
        [{"key": EVIDENCE, "version": 3}],
        {EVIDENCE: 3},
    )
    leases.release(SCOPE, DERIVED, "worker-b")
    if acquired.status.value != "ACQUIRED" or committed is None:
        os._exit(4)
    os._exit(1)


MODES = {
    "establish": establish,
    "read_only": read_only,
    "claim_and_die": claim_and_die,
    "commit_and_die": commit_and_die,
    "die_before_commit": die_before_commit,
    "effect_then_die": effect_then_die,
    "continuity_process_1": continuity_process_1,
}


if __name__ == "__main__":
    MODES[sys.argv[1]](sys.argv[2])
