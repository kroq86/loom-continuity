"""Work queue ownership: a worker heartbeats (renew) while it works, so no
other worker takes the event; a worker whose lease expired and whose event
was claimed again cannot extend, complete or fail the newer claim (claim
token = fencing token). Two processes share the database."""
import subprocess
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from support import new_db

from loom_continuity import EventStatus, SqliteEventQueue

OTHER = """
import sys
from loom_continuity import SqliteEventQueue
e = SqliteEventQueue(sys.argv[1], lease_s=1.0).claim(["job"])
print(e["claim_token"] if e else "none", flush=True)
"""


def other_claims(db: str) -> str:
    return subprocess.run([sys.executable, "-c", OTHER, db], capture_output=True, text=True).stdout.strip()


def main() -> bool:
    tmp, db = new_db()
    try:
        q = SqliteEventQueue(db, lease_s=1.0)
        job = q.publish("job", {"n": 1})
        a = q.claim(["job"])
        # A works 2.5 s with heartbeats: nobody else gets the event.
        renewed, seen = [], []
        for _ in range(5):
            time.sleep(0.5)
            renewed.append(q.renew(job, a["claim_token"]))
            seen.append(other_claims(db))
        # A stalls (no heartbeat): the lease expires, B claims it.
        time.sleep(1.2)
        b = q.claim(["job"])
        late_renew = q.renew(job, a["claim_token"])
        late_ack = q.ack(job, a["claim_token"])
        late_fail = q.fail(job, "late", claim_token=a["claim_token"])
        row = next(e for e in q.list() if e["event_id"] == job)
        b_ack = q.ack(job, b["claim_token"])
        done = next(e for e in q.list() if e["event_id"] == job)
        print(f"tokens a={a['claim_token']} b={b and b['claim_token']} renewed={renewed} other={seen} "
              f"late_renew={late_renew} late_ack={late_ack} late_fail={late_fail} "
              f"after_late={row['status']} b_ack={b_ack} final={done['status']}")
        return (all(renewed) and seen == ["none"] * 5 and b is not None and b["claim_token"] == a["claim_token"] + 1
                and late_renew is False and late_ack is False and late_fail == EventStatus.LEASE_LOST
                and row["status"] == EventStatus.PROCESSING and b_ack is True and done["status"] == EventStatus.DONE)
    finally:
        tmp.cleanup()


if __name__ == "__main__":
    raise SystemExit(0 if main() else 1)
