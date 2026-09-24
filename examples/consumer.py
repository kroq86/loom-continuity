"""External-style consumer. Run from an environment where loom-continuity is installed.

Does not import ticket_harness, FastAPI, Ollama, or Qwen.
"""
import tempfile
from pathlib import Path

from loom_continuity import SqliteOutbox, SqliteKnowledgeStore, commit_fact_with_event


def main() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        path = str(Path(tmp) / "app.sqlite3")
        memory = SqliteKnowledgeStore(path)
        outbox = SqliteOutbox(path)
        memory.put_fact("demo", "policy/billing", "billing_team", "app", [])
        version, event_id = commit_fact_with_event(
            memory, outbox, "demo", "routing/billing",
            {"team": "billing_team"}, "app",
            [{"key": "policy/billing", "version": 1}],
        )
        print({"version": version, "event_id": event_id, "status": memory.get_valid_fact("demo", "routing/billing").status})


if __name__ == "__main__":
    main()
