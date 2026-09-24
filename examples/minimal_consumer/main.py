"""Separate-project sketch. Uses only the installed public loom_continuity API."""
import tempfile
from pathlib import Path

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


def main() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        path = str(Path(tmp) / "knowledge.sqlite3")
        knowledge = SqliteKnowledgeStore(path)
        outbox = SqliteOutbox(path)
        knowledge.put_fact(SCOPE, EVIDENCE, "blue", "establish", [])
        commit_fact_with_event(
            knowledge, outbox, SCOPE, DERIVED, "monitor", "derive",
            [{"key": EVIDENCE, "version": 1}],
        )
        established = knowledge.get_valid_fact(SCOPE, DERIVED)
        knowledge.put_fact(SCOPE, EVIDENCE, "red", "mutate", [])
        stale = knowledge.get_valid_fact(SCOPE, DERIVED)
        claim = SqliteRepairLease(path).claim(SCOPE, DERIVED, "worker", {EVIDENCE: 2})
        committed = commit_fact_with_event_if_versions_match(
            knowledge, outbox, SCOPE, DERIVED, "escalate", "repair",
            [{"key": EVIDENCE, "version": 2}],
            {EVIDENCE: 2},
        )
        repaired = knowledge.get_valid_fact(SCOPE, DERIVED)
        print({
            "established": established.status,
            "after_evidence_change": stale.status,
            "preserved_revision": stale.fact.value,
            "claim": claim.status,
            "committed_version": None if committed is None else committed[0],
            "repaired": repaired.status,
            "current_value": repaired.fact.value,
        })


if __name__ == "__main__":
    main()
