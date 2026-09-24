"""One process repairs through a moving dependency and dies before dispatch. The next process finishes delivery."""
import sqlite3
import sys
from contextlib import closing
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from support import DERIVED, EVIDENCE, SCOPE, new_db, spawn

from loom_continuity import SqliteKnowledgeStore, SqliteOutbox, dispatch_pending


def main() -> bool:
    tmp, db = new_db()
    try:
        dead = spawn("continuity_process_1", db)
        if dead.returncode == 0:
            print(dead.stderr)
            return False
        knowledge = SqliteKnowledgeStore(db)
        outbox = SqliteOutbox(db)
        fact = knowledge.get_fact(SCOPE, DERIVED)
        lookup = knowledge.get_valid_fact(SCOPE, DERIVED)
        revisions = knowledge.get_revisions(SCOPE, DERIVED)
        pending = outbox.pending(SCOPE)
        newer, older = sorted(pending, key=lambda event: event.version, reverse=True)
        seen = []

        def current_state(event) -> None:
            seen.append(knowledge.get_fact(event.scope, event.key).value)

        current_state(newer)
        current_state(older)
        with closing(sqlite3.connect(db)) as conn:
            conn.execute("CREATE TABLE IF NOT EXISTS effects (event_id TEXT PRIMARY KEY)")
            conn.execute("INSERT OR IGNORE INTO effects (event_id) VALUES (?)", (newer.event_id,))
            conn.execute("INSERT OR IGNORE INTO effects (event_id) VALUES (?)", (newer.event_id,))
            conn.commit()
            effects = conn.execute("SELECT COUNT(*) FROM effects").fetchone()[0]
        delivered = dispatch_pending(outbox, lambda _event: None, SCOPE)
        print(
            f"value={fact.value} status={lookup.status} revisions={[item.version for item in revisions]} "
            f"provenance={fact.provenance} seen={seen} effects={effects} pending={len(outbox.pending(SCOPE))}"
        )
        return (
            fact.value == "escalate"
            and lookup.status == "VALID"
            and [item.value for item in revisions] == ["monitor", "escalate"]
            and revisions[-1].version == revisions[0].version + 1
            and fact.provenance == [{"key": EVIDENCE, "version": 3}]
            and "from-v2" not in [item.value for item in revisions]
            and seen == ["escalate", "escalate"]
            and effects == 1
            and len(delivered) == 2
            and outbox.pending(SCOPE) == []
        )
    finally:
        tmp.cleanup()


if __name__ == "__main__":
    raise SystemExit(0 if main() else 1)
