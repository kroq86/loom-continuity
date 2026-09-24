"""Run the ten continuity scenarios against the installed loom-continuity package."""
import subprocess
import sys
from pathlib import Path

import loom_continuity

ROOT = Path(__file__).resolve().parent / "tests" / "e2e"
NAMES = [
    "test_01_durable_reuse.py",
    "test_02_stale_preserves_history.py",
    "test_03_transitive_invalidation.py",
    "test_04_concurrent_single_flight.py",
    "test_05_moving_evidence_cas.py",
    "test_06_owner_death_recovery.py",
    "test_07_atomic_fact_outbox.py",
    "test_08_at_least_once_idempotency.py",
    "test_09_reverse_delivery_current_state.py",
    "test_10_full_continuity.py",
]


def main() -> bool:
    print(f"package={loom_continuity.__file__}")
    failed = []
    for name in NAMES:
        result = subprocess.run([sys.executable, str(ROOT / name)], capture_output=True, text=True)
        status = "PASS" if result.returncode == 0 else "FAIL"
        print(f"{status} {name}")
        if result.stdout.strip():
            print(result.stdout.strip())
        if result.returncode != 0:
            print(result.stderr.strip())
            failed.append(name)
    print(f"{len(NAMES) - len(failed)}/{len(NAMES)}")
    return not failed


if __name__ == "__main__":
    raise SystemExit(0 if main() else 1)
