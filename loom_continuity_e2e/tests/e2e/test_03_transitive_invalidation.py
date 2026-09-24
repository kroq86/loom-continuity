"""Changing A stales B and C even though B's own version row did not change."""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from support import SCOPE, new_db

from loom_continuity import SqliteKnowledgeStore


def main() -> bool:
    tmp, db = new_db()
    try:
        knowledge = SqliteKnowledgeStore(db)
        knowledge.put_fact(SCOPE, "A", "a1", "seed", [])
        knowledge.put_fact(SCOPE, "B", "b1", "derive", [{"key": "A", "version": 1}])
        knowledge.put_fact(SCOPE, "C", "c1", "derive", [{"key": "B", "version": 1}])
        b_version_before = knowledge.get_current_version(SCOPE, "B")
        knowledge.put_fact(SCOPE, "A", "a2", "mutate", [])
        b_lookup = knowledge.get_valid_fact(SCOPE, "B")
        c_lookup = knowledge.get_valid_fact(SCOPE, "C")
        print(f"B={b_lookup.status} C={c_lookup.status} B_version={knowledge.get_current_version(SCOPE, 'B')}")
        return (
            b_version_before == 1
            and knowledge.get_current_version(SCOPE, "B") == 1
            and knowledge.get_current_version(SCOPE, "C") == 1
            and b_lookup.status == "STALE"
            and c_lookup.status == "STALE"
        )
    finally:
        tmp.cleanup()


if __name__ == "__main__":
    raise SystemExit(0 if main() else 1)
