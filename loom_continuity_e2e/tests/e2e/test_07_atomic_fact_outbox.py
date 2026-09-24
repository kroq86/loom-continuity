"""A crash after commit keeps both the revision and the pending event. A crash before commit keeps neither."""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from support import DERIVED, SCOPE, new_db, spawn

from loom_continuity import SqliteKnowledgeStore, SqliteOutbox, dispatch_pending


def main() -> bool:
    crash_dir, crash_db = new_db()
    control_dir, control_db = new_db()
    try:
        crashed = spawn("commit_and_die", crash_db)
        control = spawn("die_before_commit", control_db)
        if crashed.returncode == 0 or control.returncode == 0:
            print("expected os._exit(1)", crashed.returncode, control.returncode)
            return False
        knowledge = SqliteKnowledgeStore(crash_db)
        outbox = SqliteOutbox(crash_db)
        fact = knowledge.get_fact(SCOPE, DERIVED)
        pending = outbox.pending(SCOPE)
        delivered = dispatch_pending(outbox, lambda _event: None, SCOPE)
        control_fact = SqliteKnowledgeStore(control_db).get_fact(SCOPE, DERIVED)
        control_pending = SqliteOutbox(control_db).pending(SCOPE)
        print(f"after_crash fact={fact.value if fact else None} pending_before_dispatch={len(pending)} delivered={len(delivered)}")
        print(f"before_commit fact={control_fact} pending={len(control_pending)}")
        return (
            fact is not None
            and fact.value == "monitor"
            and len(pending) == 1
            and len(delivered) == 1
            and control_fact is None
            and control_pending == []
        )
    finally:
        crash_dir.cleanup()
        control_dir.cleanup()


if __name__ == "__main__":
    raise SystemExit(0 if main() else 1)
