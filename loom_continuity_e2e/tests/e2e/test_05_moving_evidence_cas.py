"""A result computed against v2 is rejected after the dependency moves to v3."""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from support import DERIVED, EVIDENCE, SCOPE, new_db

from loom_continuity import SqliteKnowledgeStore, SqliteOutbox, SqliteRepairLease, commit_fact_with_event, commit_fact_with_event_if_versions_match


def main() -> bool:
    tmp, db = new_db()
    try:
        knowledge = SqliteKnowledgeStore(db)
        outbox = SqliteOutbox(db)
        knowledge.put_fact(SCOPE, EVIDENCE, "v1", "establish", [])
        commit_fact_with_event(
            knowledge, outbox, SCOPE, DERIVED, "monitor", "derive",
            [{"key": EVIDENCE, "version": 1}],
        )
        knowledge.put_fact(SCOPE, EVIDENCE, "v2", "mutate", [])
        claim = SqliteRepairLease(db).claim(SCOPE, DERIVED, "worker-a", {EVIDENCE: 2}, lease_seconds=30)
        knowledge.put_fact(SCOPE, EVIDENCE, "v3", "mutate-during-repair", [])
        rejected = commit_fact_with_event_if_versions_match(
            knowledge, outbox, SCOPE, DERIVED, "from-v2", "stale-repair",
            [{"key": EVIDENCE, "version": 2}],
            {EVIDENCE: 2},
        )
        SqliteRepairLease(db).release(SCOPE, DERIVED, "worker-a")
        next_claim = SqliteRepairLease(db).claim(SCOPE, DERIVED, "worker-b", {EVIDENCE: 3}, lease_seconds=30)
        committed = commit_fact_with_event_if_versions_match(
            knowledge, outbox, SCOPE, DERIVED, "from-v3", "repair",
            [{"key": EVIDENCE, "version": 3}],
            {EVIDENCE: 3},
        )
        revisions = knowledge.get_revisions(SCOPE, DERIVED)
        lookup = knowledge.get_valid_fact(SCOPE, DERIVED)
        print(f"rejected={rejected} committed={committed} values={[r.value for r in revisions]} status={lookup.status}")
        return (
            claim.status.value == "ACQUIRED"
            and rejected is None
            and "from-v2" not in [item.value for item in revisions]
            and next_claim.status.value == "ACQUIRED"
            and committed is not None
            and lookup.status == "VALID"
            and lookup.fact.value == "from-v3"
            and lookup.fact.provenance == [{"key": EVIDENCE, "version": 3}]
        )
    finally:
        tmp.cleanup()


if __name__ == "__main__":
    raise SystemExit(0 if main() else 1)
