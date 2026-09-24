"""Process helpers for Loom Continuity end-to-end runs."""
import json
import subprocess
import sys
import tempfile
from pathlib import Path

WORKER = Path(__file__).resolve().parent / "worker.py"
SCOPE = "job-1"
EVIDENCE = "evidence/source"
DERIVED = "derived/answer"


def spawn(*args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, str(WORKER), *args],
        capture_output=True,
        text=True,
    )


def require_ok(result: subprocess.CompletedProcess[str]) -> dict:
    if result.returncode != 0:
        raise AssertionError(f"worker failed ({result.returncode})\n{result.stdout}\n{result.stderr}")
    line = result.stdout.strip().splitlines()[-1]
    return json.loads(line)


def new_db() -> tuple[tempfile.TemporaryDirectory, str]:
    tmp = tempfile.TemporaryDirectory()
    return tmp, str(Path(tmp.name) / "continuity.sqlite3")
