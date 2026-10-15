"""Proofs that travel with the package."""
import sys
import tempfile
import unittest
from pathlib import Path


PUBLIC_API = {
    "LLMBackend",
    "StatefulLLMBackend",
    "FactRevision",
    "ProvenanceRef",
    "Lookup",
    "LookupStatus",
    "KnowledgeStore",
    "SqliteKnowledgeStore",
    "OutboxEvent",
    "OutboxStatus",
    "Outbox",
    "SqliteOutbox",
    "commit_fact_with_event",
    "commit_fact_with_event_if_versions_match",
    "dispatch_pending",
    "Lease",
    "ClaimStatus",
    "ClaimResult",
    "RepairLeaseStore",
    "SqliteRepairLease",
    "WorkQueue",
    "SqliteEventQueue",
    "ClaimedEvent",
    "EventStatus",
}


class ImportBoundaryTest(unittest.TestCase):
    def test_public_surface_matches_frozen_snapshot(self):
        import loom_continuity

        self.assertEqual(set(loom_continuity.__all__), PUBLIC_API)
        for name in ("Fact", "RepairLease", "LeaseDenied", "SessionMemory", "DurableKnowledge"):
            self.assertNotIn(name, loom_continuity.__all__)
            self.assertFalse(hasattr(loom_continuity, name))

    def test_root_import_pulls_no_adapter_or_app(self):
        import loom_continuity
        loaded = set(sys.modules)
        self.assertIn("loom_continuity.knowledge.protocol", loaded)
        self.assertIn("loom_continuity.knowledge.sqlite", loaded)
        banned = ("torch", "transformers", "httpx", "fastapi", "sentence_transformers")
        self.assertFalse(any(name == b or name.startswith(b + ".") for name in loaded for b in banned))
        self.assertTrue(hasattr(loom_continuity, "SqliteKnowledgeStore"))
        self.assertTrue(hasattr(loom_continuity, "LLMBackend"))


class MemoryTest(unittest.TestCase):
    def test_sqlite_adapter_satisfies_memory_protocol(self):
        from loom_continuity import KnowledgeStore, SqliteKnowledgeStore

        with tempfile.TemporaryDirectory() as tmp:
            self.assertIsInstance(SqliteKnowledgeStore(str(Path(tmp) / "m.sqlite3")), KnowledgeStore)

    def test_valid_means_dependency_versions_match(self):
        from loom_continuity import SqliteKnowledgeStore

        with tempfile.TemporaryDirectory() as tmp:
            memory = SqliteKnowledgeStore(str(Path(tmp) / "m.sqlite3"))
            memory.put_fact("s", "color", "blue", "seed", [])
            v = memory.get_current_version("s", "color")
            memory.put_fact("s", "action", "monitor", "derive", [{"key": "color", "version": v}])
            self.assertEqual(memory.get_valid_fact("s", "action").status, "VALID")
            memory.put_fact("s", "color", "red", "mutate", [])
            lookup = memory.get_valid_fact("s", "action")
            self.assertEqual(lookup.status, "STALE")
            self.assertEqual(lookup.fact.value, "monitor")


class OutboxTest(unittest.TestCase):
    def test_sqlite_adapter_satisfies_outbox_protocol(self):
        from loom_continuity import Outbox, SqliteOutbox

        with tempfile.TemporaryDirectory() as tmp:
            self.assertIsInstance(SqliteOutbox(str(Path(tmp) / "o.sqlite3")), Outbox)

    def test_commit_is_atomic_and_cas_rejects_moved_evidence(self):
        from loom_continuity import SqliteOutbox, SqliteKnowledgeStore, commit_fact_with_event, commit_fact_with_event_if_versions_match

        with tempfile.TemporaryDirectory() as tmp:
            path = str(Path(tmp) / "o.sqlite3")
            memory = SqliteKnowledgeStore(path)
            outbox = SqliteOutbox(path)
            memory.put_fact("s", "policy", "platform", "seed", [])
            version, event_id = commit_fact_with_event(
                memory, outbox, "s", "route", {"team": "platform"}, "compute",
                [{"key": "policy", "version": 1}],
            )
            self.assertEqual(version, 1)
            self.assertEqual(len(outbox.pending("s")), 1)
            self.assertEqual(outbox.get(event_id).version, 1)

            memory.put_fact("s", "policy", "escalations", "change", [])
            rejected = commit_fact_with_event_if_versions_match(
                memory, outbox, "s", "route", {"team": "old"}, "repair",
                [{"key": "policy", "version": 1}],
                {"policy": 1},
            )
            self.assertIsNone(rejected)
            self.assertEqual(memory.get_fact("s", "route").value["team"], "platform")
            self.assertEqual(len(outbox.pending("s")), 1)


class LeaseTest(unittest.TestCase):
    def test_sqlite_adapter_satisfies_lease_protocol(self):
        from loom_continuity import RepairLeaseStore, SqliteRepairLease

        with tempfile.TemporaryDirectory() as tmp:
            self.assertIsInstance(SqliteRepairLease(str(Path(tmp) / "l.sqlite3")), RepairLeaseStore)

    def test_second_owner_is_denied_until_release(self):
        from loom_continuity import ClaimStatus, SqliteRepairLease

        with tempfile.TemporaryDirectory() as tmp:
            path = str(Path(tmp) / "l.sqlite3")
            leases = SqliteRepairLease(path)
            first = leases.claim("s", "route", "owner-a", {"policy": 1}, lease_seconds=30)
            denied = leases.claim("s", "route", "owner-b", {"policy": 1}, lease_seconds=30)
            self.assertEqual(first.status, ClaimStatus.ACQUIRED)
            self.assertEqual(denied.status, ClaimStatus.DENIED)
            leases.release("s", "route", "owner-a")
            acquired = leases.claim("s", "route", "owner-b", {"policy": 2}, lease_seconds=30)
            self.assertEqual(acquired.status, ClaimStatus.ACQUIRED)


if __name__ == "__main__":
    unittest.main()
