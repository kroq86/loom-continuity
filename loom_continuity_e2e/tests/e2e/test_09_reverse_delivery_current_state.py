"""A delayed older notification re-reads current knowledge and does not restore the old value."""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from support import DERIVED, EVIDENCE, SCOPE, new_db

from loom_continuity import SqliteKnowledgeStore, SqliteOutbox, commit_fact_with_event


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
        commit_fact_with_event(
            knowledge, outbox, SCOPE, DERIVED, "escalate", "repair",
            [{"key": EVIDENCE, "version": 2}],
        )
        older, newer = outbox.pending(SCOPE)
        seen = []

        def deliver(event) -> None:
            current = knowledge.get_fact(event.scope, event.key)
            seen.append((event.version, current.value))
            outbox.mark_delivered(event.event_id)

        deliver(newer)
        deliver(older)
        print(seen)
        return seen == [(2, "escalate"), (1, "escalate")] and "monitor" not in [value for _version, value in seen]
    finally:
        tmp.cleanup()


if __name__ == "__main__":
    raise SystemExit(0 if main() else 1)
