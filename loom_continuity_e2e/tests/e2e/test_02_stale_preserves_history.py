"""A newer evidence revision makes the derived fact stale without deleting the old revision."""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from support import DERIVED, EVIDENCE, SCOPE, new_db

from loom_continuity import SqliteKnowledgeStore, SqliteOutbox, commit_fact_with_event


def main() -> bool:
    tmp, db = new_db()
    try:
        knowledge = SqliteKnowledgeStore(db)
        knowledge.put_fact(SCOPE, EVIDENCE, "blue", "establish", [])
        commit_fact_with_event(
            knowledge, SqliteOutbox(db), SCOPE, DERIVED, "monitor", "derive",
            [{"key": EVIDENCE, "version": 1}],
        )
        knowledge.put_fact(SCOPE, EVIDENCE, "red", "mutate", [])
        revisions = knowledge.get_revisions(SCOPE, DERIVED)
        lookup = knowledge.get_valid_fact(SCOPE, DERIVED)
        print(f"revisions={[ (r.version, r.value) for r in revisions ]} status={lookup.status}")
        return (
            len(revisions) == 1
            and revisions[0].version == 1
            and revisions[0].value == "monitor"
            and lookup.status == "STALE"
            and lookup.fact.value == "monitor"
        )
    finally:
        tmp.cleanup()


if __name__ == "__main__":
    raise SystemExit(0 if main() else 1)
