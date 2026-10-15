"""Work queue: a worker process dies after claiming; the event is withheld while
its lease lives, then redelivered to another process. Failures back off, then
park the event as dead with its last error."""
import subprocess
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from support import new_db

from loom_continuity import EventStatus, SqliteEventQueue

DIES = """
import os, sys
from loom_continuity import SqliteEventQueue
event = SqliteEventQueue(sys.argv[1], lease_s=1.0).claim(["job.ingest"])
print(event["attempt"] if event else "none", flush=True)
os._exit(1)
"""


def main() -> bool:
    tmp, db = new_db()
    try:
        queue = SqliteEventQueue(db, max_attempts=3, lease_s=1.0, base_backoff_s=0.5)
        job = queue.publish("job.ingest", {"blob": "raw/a.pdf"}, correlation_id="trace-1")
        queue.publish("job.other", {"blob": "raw/b.pdf"})

        died = subprocess.run([sys.executable, "-c", DIES, db], capture_output=True, text=True, check=False)
        withheld = queue.claim(["job.ingest"])
        time.sleep(1.2)
        redelivered = queue.claim(["job.ingest"])

        first_fail = queue.fail(job, "TimeoutError: parser")
        during_backoff = queue.claim(["job.ingest"])
        time.sleep(1.1)  # backoff after attempt 2 is 0.5 * 2^1 = 1.0 s
        third = queue.claim(["job.ingest"])
        final_fail = queue.fail(job, "ValueError: corrupt pdf")
        dead = queue.list(status=EventStatus.DEAD)

        permanent = queue.publish("job.ingest", {"blob": "raw/corrupt.pdf"})
        claimed = queue.claim(["job.ingest"])
        permanent_status = queue.fail(permanent, "FileDataError: corrupt", retry=False) \
            if claimed and claimed["event_id"] == permanent else "not claimed"
        permanent_row = next(e for e in queue.list(status=EventStatus.DEAD) if e["event_id"] == permanent)

        other = queue.claim(["job.other"])
        queue.ack(other["event_id"])
        done = queue.list(status=EventStatus.DONE)

        print(f"died_attempt={died.stdout.strip()} rc={died.returncode} withheld={withheld} "
              f"redelivered_attempt={redelivered and redelivered['attempt']} first_fail={first_fail} "
              f"during_backoff={during_backoff} third_attempt={third and third['attempt']} final_fail={final_fail} "
              f"dead={[(d['event_id'] == job, d['attempts'], d['last_error']) for d in dead]} done={len(done)} "
              f"permanent={permanent_status} after {permanent_row['attempts']} attempt(s)")
        return (
            died.stdout.strip() == "1" and died.returncode == 1
            and withheld is None
            and redelivered is not None and redelivered["event_id"] == job and redelivered["attempt"] == 2
            and redelivered["correlation_id"] == "trace-1" and redelivered["payload"] == {"blob": "raw/a.pdf"}
            and first_fail == EventStatus.PENDING
            and during_backoff is None
            and third is not None and third["attempt"] == 3
            and final_fail == EventStatus.DEAD
            and len(dead) == 1 and dead[0]["event_id"] == job and dead[0]["last_error"] == "ValueError: corrupt pdf"
            and len(done) == 1 and done[0]["event_id"] == other["event_id"]
            and permanent_status == EventStatus.DEAD and permanent_row["attempts"] == 1
        )
    finally:
        tmp.cleanup()


if __name__ == "__main__":
    raise SystemExit(0 if main() else 1)
