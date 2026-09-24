"""The consumer records the effect and dies before ack. Redelivery does not apply it twice."""
import sqlite3
import sys
from contextlib import closing
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from support import DERIVED, EVIDENCE, SCOPE, new_db, spawn

from loom_continuity import SqliteKnowledgeStore, SqliteOutbox, commit_fact_with_event, dispatch_pending


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
        dead = spawn("effect_then_die", db)
        if dead.returncode == 0:
            return False
        attempts = {"n": 1}

        def consumer(event) -> None:
            attempts["n"] += 1
            with closing(sqlite3.connect(db)) as conn:
                conn.execute("INSERT OR IGNORE INTO effects (event_id) VALUES (?)", (event.event_id,))
                conn.commit()

        delivered = dispatch_pending(outbox, consumer, SCOPE)
        with closing(sqlite3.connect(db)) as conn:
            effects = conn.execute("SELECT COUNT(*) FROM effects").fetchone()[0]
        pending = outbox.pending(SCOPE)
        print(f"attempts={attempts['n']} effects={effects} delivered={len(delivered)} pending={len(pending)}")
        return attempts["n"] >= 2 and effects == 1 and len(delivered) == 1 and pending == []
    finally:
        tmp.cleanup()


if __name__ == "__main__":
    raise SystemExit(0 if main() else 1)
