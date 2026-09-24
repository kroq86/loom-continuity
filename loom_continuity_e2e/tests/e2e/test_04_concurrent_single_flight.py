"""Twenty workers see one stale key. Only the lease holder writes the repair."""
import sys
import threading
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from support import DERIVED, EVIDENCE, SCOPE, new_db

from loom_continuity import ClaimStatus, SqliteKnowledgeStore, SqliteOutbox, SqliteRepairLease, commit_fact_with_event, commit_fact_with_event_if_versions_match


def main() -> bool:
    tmp, db = new_db()
    try:
        knowledge = SqliteKnowledgeStore(db)
        outbox = SqliteOutbox(db)
        knowledge.put_fact(SCOPE, EVIDENCE, "blue", "establish", [])
        commit_fact_with_event(
            knowledge, outbox, SCOPE, DERIVED, "monitor", "derive",
            [{"key": EVIDENCE, "version": 1}],
        )
        knowledge.put_fact(SCOPE, EVIDENCE, "red", "mutate", [])
        acquired = []
        seen = []
        lock = threading.Lock()
        start = threading.Barrier(20)
        claimed = threading.Barrier(20)

        def worker(name: str) -> None:
            leases = SqliteRepairLease(db)
            start.wait()
            result = leases.claim(SCOPE, DERIVED, name, {EVIDENCE: 2}, lease_seconds=30)
            with lock:
                if result.status == ClaimStatus.ACQUIRED:
                    acquired.append(name)
            claimed.wait()
            if result.status == ClaimStatus.ACQUIRED:
                commit_fact_with_event_if_versions_match(
                    SqliteKnowledgeStore(db), SqliteOutbox(db), SCOPE, DERIVED, "escalate", "repair",
                    [{"key": EVIDENCE, "version": 2}],
                    {EVIDENCE: 2},
                )
                leases.release(SCOPE, DERIVED, name)
            else:
                for _ in range(100):
                    lookup = SqliteKnowledgeStore(db).get_valid_fact(SCOPE, DERIVED)
                    if lookup.status == "VALID" and lookup.fact.value == "escalate":
                        with lock:
                            seen.append(lookup.fact.value)
                        return
                with lock:
                    seen.append(None)

        threads = [threading.Thread(target=worker, args=(f"worker-{i}",)) for i in range(20)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        revisions = knowledge.get_revisions(SCOPE, DERIVED)
        repair_events = [event for event in SqliteOutbox(db).pending(SCOPE) if event.key == DERIVED and event.version == 2]
        print(f"acquired={acquired} losers_saw={set(seen)} revisions={[r.value for r in revisions]} repair_events={len(repair_events)}")
        return (
            len(acquired) == 1
            and seen.count("escalate") == 19
            and [item.value for item in revisions] == ["monitor", "escalate"]
            and len(repair_events) == 1
        )
    finally:
        tmp.cleanup()


if __name__ == "__main__":
    raise SystemExit(0 if main() else 1)
