# Loom Continuity

`loom_continuity` is a durable coordination and knowledge-state runtime.
It is not an agent framework.
It is designed to sit underneath application harnesses and agent frameworks.

It answers one question: a value was derived from some evidence; is that evidence still current, and if not, who may recompute it without duplicating the work, committing a stale result, or losing the notice that state changed.

The package does not choose the application shape. A caller reads knowledge, checks validity, claims a repair, commits only if the evidence versions still match, and leaves a durable notification. A web app, a worker, a graph runtime, a batch job, or a plain function decides when to make those calls.

## Using it

The first mode is a library inside one process. An application opens `SqliteKnowledgeStore`, `SqliteOutbox`, and `SqliteRepairLease` on one database file. A request looks up a fact. `VALID` returns the stored value. `MISSING` computes it. `STALE` claims the lease, computes outside the database transaction, and commits only if the dependency versions are unchanged. FastAPI, or any other process, is just a client of that runtime. Importing the package does not pull FastAPI in.

The production shape splits roles across processes that share the file. The request process writes evidence and can return before the expensive work. A worker finds stale or missing facts, claims the lease, computes, and commits. A dispatcher delivers pending outbox events and re-reads the current value. Any one of those processes can die. The fact, the pending obligation, and the lease expiry remain in the file.

Several processes can see the same stale key. One claim is `ACQUIRED`. The others are `DENIED` until the lease expires or is released, then they read the repaired value. That is the coordination, not a database wrapper. The SQLite implementation is for one shared file. Several machines should not share that file. The protocols are `KnowledgeStore`, `Outbox`, and `RepairLeaseStore`; a later backend would be a new implementation, not a change to the caller's questions.

An agent framework sits above this. A graph or agent node asks for a fact and skips the expensive chain while the lookup is `VALID`. It runs only when the fact is `STALE` or `MISSING`, then writes the result with provenance. Reuse lasts until the evidence version changes. It is not a time-to-live cache.

A batch job can do the same without a server: `get_valid_fact` for each item, skip what is still `VALID`, repair what is `STALE`, compute what is `MISSING`. A scheduler such as Temporal or Celery can start the activity. This package does not replace that scheduler. The scheduler decides when to run. The runtime decides whether the result is still current and whether the write is still safe.

A later HTTP service can wrap the same calls for other languages. That service is not this package. Callers that can import Python should use the library.

An agent tool can hide the protocol and return the current fact, or report `STALE` and leave repair to an orchestrator. Several agents can share one store. When evidence moves, the derived fact becomes `STALE` for all of them. That is shared institutional knowledge, not conversation history.

The compute step can be a Python function. Nothing in the core requires a model.

The distribution version is declared once, in `pyproject.toml`.
The base install has no FastAPI, torch, or model-runtime dependency.

## What it guarantees

- Versioned knowledge survives process death. A later process can reopen the same store.
- `LookupStatus.VALID` means every provenance dependency version is still current, including transitive dependencies.
- A fact whose dependency moved is `STALE`. The stored revision is not deleted.
- If `A -> B -> C` and `A` changes, `C` is stale even when `B`'s revision number has not changed.
- The store does not repair itself. A caller claims a per-scope, per-key lease, computes outside any database transaction, and commits only if the dependency versions still match.
- A lease expires. After the owner process dies, another worker can acquire it. The lease is not held as a long transaction across the computation.
- The work queue hands out a `claim_token` with every claim (a fencing token: it grows on each claim of the event). A worker heartbeats with `renew(event_id, claim_token)` while it works; `ack` and `fail` given the token act only while that claim is current. A worker whose lease expired, and whose event was claimed again, gets `False` / `lease_lost` and changes nothing. Without a token, `ack`/`fail` behave as before 0.4 (unconditional).
- A repair computed against old dependency versions is rejected. That result never becomes the current committed value.
- A fact revision and its outbox obligation commit in one transaction. A crash after commit leaves both. A crash before commit leaves neither.
- Delivery is at-least-once. Pending events remain until the consumer returns.
- Reopen, stale detection, single-owner repair, rejected stale commits, crash boundaries, and current-state delivery are covered by `loom_continuity_e2e`.

## What it does not guarantee

- `VALID` does not mean semantically correct, true, safe, approved, or high confidence. A wrong value is `VALID` while its dependency chain is current.
- Delivery is not exactly-once.
- The package does not deduplicate consumer side effects.
- Outbox events are not an event log of authoritative values. This is not event sourcing.
- There is no agent loop, tool framework, conversation memory, or semantic/vector memory.
- There is no auth, UI, or deployment framework.

## Not in this cut

This is one SQLite file and process coordination around it. It is not a city. These are outside the frozen contract:

- identity service
- permissions
- scheduler
- service discovery
- policy enforcement beyond provenance freshness
- a second storage backend
- coordination across nodes

`KnowledgeStore`, `Outbox`, `RepairLeaseStore`, and `WorkQueue` are protocols with a SQLite implementation. Another backend is an open slot, not a delivered adapter. `scope` is a namespace string, not an identity. Lease TTL is an expiry, not a scheduler.

## `scope`

`scope` is the isolation namespace for independent knowledge state.
It may name a tenant, workflow, project, incident, job, or another application domain.
It is not a conversation id.

Public methods take `scope`. The SQLite primary key column is already `scope`. There is no `session_id` column and no schema migration in this cut.

## Public import surface

```python
from loom_continuity import (
    KnowledgeStore,
    SqliteKnowledgeStore,
    FactRevision,
    ProvenanceRef,
    Lookup,
    LookupStatus,
    Outbox,
    SqliteOutbox,
    OutboxEvent,
    OutboxStatus,
    commit_fact_with_event,
    commit_fact_with_event_if_versions_match,
    dispatch_pending,
    RepairLeaseStore,
    SqliteRepairLease,
    Lease,
    ClaimStatus,
    ClaimResult,
    WorkQueue,
    SqliteEventQueue,
    ClaimedEvent,
    EventStatus,
    LLMBackend,
    StatefulLLMBackend,
)
```

`KnowledgeStore` is the storage protocol. `SqliteKnowledgeStore` is the SQLite implementation.
`FactRevision` is the only fact value type. There is no separate `Fact` alias.
`Lease` is the only lease value type. `claim` returns `ClaimStatus.DENIED` and does not raise.
`commit_fact_with_event` and `commit_fact_with_event_if_versions_match` are the public atomic write and conditional-commit operations. They are not private helpers.
`LLMBackend` and `StatefulLLMBackend` are dependency-free call contracts. Model runtimes are not part of the base package.

## Work queue

The outbox says "this fact changed". The work queue says "do this job":
an upload to ingest, a document to re-embed, a report to build. It has the
same crash model: one SQLite file shared by the processes that publish
and the workers that consume.

```python
from loom_continuity import SqliteEventQueue

queue = SqliteEventQueue("state.sqlite3", max_attempts=3, lease_s=60, base_backoff_s=1)
queue.publish("document.uploaded", {"blob": "raw/2026/report.pdf", "tenant": "t1"}, correlation_id=trace_id)

event = queue.claim(["document.uploaded"])      # None when nothing is ready
if event is not None:
    try:
        ingest(event["payload"])                # must be idempotent
        queue.ack(event["event_id"])
    except Exception as e:
        queue.fail(event["event_id"], f"{type(e).__name__}: {e}")   # "pending" or "dead"
        # a failure that will repeat (corrupt file, unknown schema): retry=False -> "dead" at once

queue.list(status="dead")                       # the dead-letter list, with last_error
```

- **At-least-once.** A worker that dies after `claim` loses its lease. Once
  the lease expires, another worker claims the same event with `attempt == 2`.
  Consumers must be idempotent.
- **Retries.** `fail` returns the event after `base_backoff_s * 2^(attempt-1)`
  seconds. After `max_attempts` failures it is parked as `dead` with its last
  error for a human to inspect. `fail(..., retry=False)` parks a permanent
  failure (the same input will fail the same way) at once.
- **No ordering.** A retried event can overtake a newer one.
- **References, not payloads.** An event names where the data is (a blob
  path, a row id). The data itself stays in storage.
- `schema_version` travels with every event, so a consumer can refuse a
  version it does not understand instead of misreading it.

## Current-State Notification Semantics

An outbox event identifies that state changed. It carries `scope`, `key`, `version`, and a stable `event_id`. It does not carry the authoritative fact value.

The consumer re-reads the current value from `SqliteKnowledgeStore`.
If revision 1 then revision 2 were committed, delivering the newer event and then the older event still reads revision 2 both times.
A delayed older notification cannot restore an older value.

This is not event sourcing.

## Consumer idempotency

Dispatch is at-least-once. The same `event_id` can be handed to the consumer more than once, including after a crash before acknowledgement.

The package does not suppress that repeat. A consumer that must apply a logical effect once deduplicates on `event_id` itself, for example with `INSERT OR IGNORE`.

## Install and acceptance

From a clean environment, install the package, then run the acceptance suite from a checkout of this repository. The suite is not inside the wheel, and the runner does not put the source tree on `PYTHONPATH`.

```bash
python -m venv /tmp/loom-continuity-accept
source /tmp/loom-continuity-accept/bin/activate
pip install loom-continuity
python loom_continuity_e2e/run_all.py
```

Expected result: `10/10 PASS`, and `loom_continuity.__file__` under that environment's `site-packages`.

Package-local unit checks, from this directory:

```bash
python -m unittest discover -s tests -v
```

A separate minimal consumer lives at `examples/minimal_consumer/` in the repository. It imports only the public package API.
