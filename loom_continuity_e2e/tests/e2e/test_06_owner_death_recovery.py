"""The lease holder process exits. Another process waits out the TTL and finishes the repair."""
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from support import DERIVED, EVIDENCE, SCOPE, new_db, spawn

from loom_continuity import ClaimStatus, SqliteKnowledgeStore, SqliteOutbox, SqliteRepairLease, commit_fact_with_event, commit_fact_with_event_if_versions_match


def main() -> bool:
    tmp, db = new_db()
    try:
        knowledge = SqliteKnowledgeStore(db)
        outbox = SqliteOutbox(db)
        SqliteRepairLease(db)
        knowledge.put_fact(SCOPE, EVIDENCE, "blue", "establish", [])
        commit_fact_with_event(
            knowledge, outbox, SCOPE, DERIVED, "monitor", "derive",
            [{"key": EVIDENCE, "version": 1}],
        )
        knowledge.put_fact(SCOPE, EVIDENCE, "red", "mutate", [])
        dead = spawn("claim_and_die", db)
        if dead.returncode != 0:
            print(dead.stderr)
            return False
        early = SqliteRepairLease(db).claim(SCOPE, DERIVED, "worker-b", {EVIDENCE: 2}, lease_seconds=30)
        time.sleep(1.2)
        late = SqliteRepairLease(db).claim(SCOPE, DERIVED, "worker-b", {EVIDENCE: 2}, lease_seconds=30)
        committed = commit_fact_with_event_if_versions_match(
            knowledge, outbox, SCOPE, DERIVED, "escalate", "repair",
            [{"key": EVIDENCE, "version": 2}],
            {EVIDENCE: 2},
        )
        lookup = knowledge.get_valid_fact(SCOPE, DERIVED)
        print(f"early={early.status} late={late.status} committed={committed} status={lookup.status}")
        return (
            early.status == ClaimStatus.DENIED
            and late.status == ClaimStatus.ACQUIRED
            and committed is not None
            and lookup.status == "VALID"
            and lookup.fact.version == 2
            and lookup.fact.value == "escalate"
        )
    finally:
        tmp.cleanup()


if __name__ == "__main__":
    raise SystemExit(0 if main() else 1)
