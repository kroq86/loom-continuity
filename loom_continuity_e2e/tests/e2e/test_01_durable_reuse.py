"""Process 1 writes knowledge and exits. Process 2 reopens the same file and only reads."""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from support import new_db, require_ok, spawn


def main() -> bool:
    tmp, db = new_db()
    try:
        first = require_ok(spawn("establish", db))
        second = require_ok(spawn("read_only", db))
        print("process1", first)
        print("process2", second)
        return (
            second["writes"] == 0
            and second["value"] == first["value"] == "monitor"
            and second["version"] == first["version"] == 1
            and second["provenance"] == first["provenance"]
            and second["status"] == "VALID"
        )
    finally:
        tmp.cleanup()


if __name__ == "__main__":
    raise SystemExit(0 if main() else 1)
