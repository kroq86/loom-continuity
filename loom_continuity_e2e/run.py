"""End-to-end run of Loom Continuity.

Lives outside the package and outside ticket_harness tests. Uses only
loom_continuity: no FastAPI, Ollama, or Qwen.

    python loom_continuity_e2e/run.py
"""
import tempfile
from pathlib import Path

from loom_continuity import (
    ClaimStatus,
    SqliteKnowledgeStore,
    SqliteOutbox,
    SqliteRepairLease,
    commit_fact_with_event,
    commit_fact_with_event_if_versions_match,
    dispatch_pending,
)

SCOPE = "job-1"
EVIDENCE = "evidence/source"
DERIVED = "derived/answer"


def main() -> bool:
    with tempfile.TemporaryDirectory() as tmp:
        path = str(Path(tmp) / "continuity.sqlite3")
        knowledge = SqliteKnowledgeStore(path)
        outbox = SqliteOutbox(path)
        leases = SqliteRepairLease(path)

        knowledge.put_fact(SCOPE, EVIDENCE, "blue", "establish", [])
        evidence_v1 = knowledge.get_current_version(SCOPE, EVIDENCE)
        version, first_event = commit_fact_with_event(
            knowledge, outbox, SCOPE, DERIVED, "monitor", "derive",
            [{"key": EVIDENCE, "version": evidence_v1}],
        )
        print(f"established derived v{version} from {EVIDENCE}@{evidence_v1}: VALID="
              f"{knowledge.get_valid_fact(SCOPE, DERIVED).status == 'VALID'}")

        knowledge.put_fact(SCOPE, EVIDENCE, "red", "mutate", [])
        stale = knowledge.get_valid_fact(SCOPE, DERIVED)
        print(f"evidence changed: derived status={stale.status} stored_value={stale.fact.value!r}")
        if stale.status != "STALE" or stale.fact.value != "monitor":
            return False

        evidence_v2 = knowledge.get_current_version(SCOPE, EVIDENCE)
        held = leases.claim(SCOPE, DERIVED, "worker-a", {EVIDENCE: evidence_v2}, lease_seconds=30)
        denied = leases.claim(SCOPE, DERIVED, "worker-b", {EVIDENCE: evidence_v2}, lease_seconds=30)
        print(f"lease: worker-a={held.status.value} worker-b={denied.status.value}")
        if held.status != ClaimStatus.ACQUIRED or denied.status != ClaimStatus.DENIED:
            return False

        knowledge.put_fact(SCOPE, EVIDENCE, "green", "mutate-during-repair", [])
        rejected = commit_fact_with_event_if_versions_match(
            knowledge, outbox, SCOPE, DERIVED, "escalate", "repair-stale",
            [{"key": EVIDENCE, "version": evidence_v2}],
            {EVIDENCE: evidence_v2},
        )
        print(f"commit against moved evidence: {rejected}")
        if rejected is not None:
            return False
        leases.release(SCOPE, DERIVED, "worker-a")

        evidence_v3 = knowledge.get_current_version(SCOPE, EVIDENCE)
        again = leases.claim(SCOPE, DERIVED, "worker-b", {EVIDENCE: evidence_v3}, lease_seconds=30)
        committed = commit_fact_with_event_if_versions_match(
            knowledge, outbox, SCOPE, DERIVED, "escalate", "repair",
            [{"key": EVIDENCE, "version": evidence_v3}],
            {EVIDENCE: evidence_v3},
        )
        leases.release(SCOPE, DERIVED, "worker-b")
        print(f"repair after release: claim={again.status.value} commit={committed}")
        if again.status != ClaimStatus.ACQUIRED or committed is None:
            return False
        if knowledge.get_valid_fact(SCOPE, DERIVED).status != "VALID":
            return False
        if knowledge.get_fact(SCOPE, DERIVED).value != "escalate":
            return False

        reopened = SqliteOutbox(path)
        pending = reopened.pending(SCOPE)
        print(f"reopened process sees pending events: {len(pending)}")
        if len(pending) != 2:
            return False

        observed = []

        def consumer(event):
            current = SqliteKnowledgeStore(path).get_fact(event.scope, event.key)
            observed.append((event.version, None if current is None else current.value))

        newer, older = pending[1], pending[0]
        consumer(newer)
        reopened.mark_delivered(newer.event_id)
        consumer(older)
        reopened.mark_delivered(older.event_id)
        print(f"reverse delivery read current values: {observed}")
        if observed != [(committed[0], "escalate"), (version, "escalate")]:
            return False

        leftover = dispatch_pending(SqliteOutbox(path), consumer, scope=SCOPE)
        print(f"second dispatch delivered: {leftover}")
        if leftover:
            return False
    print("PASS")
    return True


if __name__ == "__main__":
    raise SystemExit(0 if main() else 1)
