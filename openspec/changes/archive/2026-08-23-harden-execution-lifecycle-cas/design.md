## Context

See `proposal.md` for motivation. Execution semantic state is stored in S3 as an independently CAS-updated `state.json`; driver ownership and adapter continuation are stored in a separately CAS-updated `driver.json`. Current state mutations share one permissive helper, and several writers either omit lifecycle preconditions or retry only the state write when the complete decision also depends on edge listings. The existing three-object execution schema, lifecycle values, cache pointers, and edge objects must remain unchanged.

The driver lock is a leased coordination convention across a separate object, not an atomic transaction with `state.json`. This design interprets “requires the lock” as requiring the writer to hold and verify the current driver owner before each state CAS attempt. State CAS and exact lifecycle preconditions remain the final protection against stale semantic writes.

## Goals / Non-Goals

**Goals:**

- Make field authority and lifecycle transition checks unavoidable for every state writer.
- Keep result and lineage publication independent from the driver lock while making each retry lifecycle-aware.
- Linearize caller registration against caller cancellation before adapter invocation.
- Make cancellation planning retry its complete lifecycle-and-edge decision.
- Preserve terminal-child lineage when caller cancellation wins concurrently.
- Improve Phase 2 observability without invoking cancellation for records outside `cancel-pending`.

**Non-Goals:**

- Changing persisted JSON shapes, object paths, adapter protocol, or public method signatures.
- Adding lock fencing fields, distributed transactions, or driver-lock renewal.
- Preventing harmless unreachable local immutable objects when cancellation races local DAG construction; authoritative execution result publication remains guarded.
- Addressing executor teardown verification, Docker inspection diagnostics, nested executor state ownership, or normal cleanup scheduling.

## Decisions

### 1. Validate state mutations by changed fields and source lifecycle

The state mutation primitive will snapshot the original state, apply one named mutation, derive the changed fields excluding the derived `updated_at`, and reject any field set or lifecycle transition outside that mutation's declared authority before issuing the conditional put. Every CAS conflict reruns the entire operation against a fresh snapshot.

Lock-free mutation kinds are limited to:

| Mutation | Fields | Source lifecycle |
| --- | --- | --- |
| Publish runtime result | `result_ref`, `result_source` | `running` |
| Register spawned child | `spawned_execution_ids` | `running` |
| Complete normal child | `spawned_execution_ids`, `child_execution_ids` | `running`, `cancel-pending` |

All lifecycle and control mutations require an owner argument and verification against the latest `driver.json` before every state CAS attempt. Mixed mutations, such as publishing an adapter-error result while changing `running -> failed`, use locked authority for the whole atomic state update.

Alternative: retain callback-specific checks only. Rejected because a new or overlooked writer could bypass the hard contract.

Alternative: require the driver lock for every state write. Rejected because runtime result publication and caller-owned lineage are intentionally independent and may occur while another process drives the callee adapter.

### 2. Encode one explicit lifecycle transition table

The state mutation layer will accept only:

```text
pending -> running
pending -> cancel-pending
running -> succeeded
running -> failed
running -> cancel-pending
cancel-pending -> canceled
```

Unchanged lifecycle is permitted only when the mutation kind authorizes its changed non-lifecycle fields. `succeeded`, `failed`, and `canceled` cannot transition. Existing invalidation and cleanup behavior does not reopen lifecycle: invalidation is a locked control-field write, while cleanup updates `driver.json`, not lifecycle or result state.

Alternative: let each writer define arbitrary source and destination sets. Rejected because duplicated transition policy is the current source of inconsistency.

### 3. Keep lock ownership in `driver.json` without a schema migration

Locked semantic writers already execute while holding the driver lock. The mutation helper will additionally require the owner token and verify the latest driver owner before each attempted state CAS. A lost owner fails before retrying. The state CAS still determines semantic ordering if lock lease expiry overlaps an operation.

This does not provide an atomic fence across the two S3 objects. A strict cross-object fencing proof would require a generation token in `state.json`, which conflicts with the no-schema-change constraint. Exact lifecycle CAS guards ensure that even a stale owner cannot overwrite cancellation or an absorbing terminal lifecycle after another semantic writer updates state.

Alternative: add a fencing generation to both objects. Rejected for this change because it changes the persisted schema and requires migration semantics.

### 4. Treat registration as edge plus caller-summary publication before invoke

While holding the callee driver lock, registration will validate the callee, publish the canonical edge, then CAS-add the callee ID to the caller's spawned summary only if the latest caller lifecycle is `running`. The adapter is called only after both publications succeed.

If caller-summary publication fails, registration removes the attempted edge. For a fresh reservation it also conditionally deletes the matching cache pointer and unchanged owned execution parts. Uploaded argument objects are content-addressed and shared, so they are preserved. Scratch objects need no cleanup because the adapter was never invoked.

The edge remains first so cancellation evaluating the callee under the same callee lock cannot select an execution whose registration has won. The guarded caller-summary CAS ensures caller cancellation either sees the spawned child or wins and prevents invocation.

Alternative: publish the spawned summary before the edge. Rejected because caller cancellation could enqueue and inspect the callee before its live incoming reference is visible.

### 5. Separate normal terminal bookkeeping from initial spawning

Initial child registration remains valid only for `running`. Normal terminal bookkeeping uses one lock-free CAS to remove the ID from spawned lineage and add it to completed lineage. It is valid for `running` and `cancel-pending` callers. When the returned caller lifecycle is `cancel-pending`, the call path raises cancellation only after the bookkeeping CAS succeeds.

Retry and adapter exceptions do not establish normal terminal completion, so the child remains spawned. A canceled child also remains spawned under the existing cancellation-lineage contract.

Alternative: skip bookkeeping after caller cancellation. Rejected because it leaves forward lineage dependent on race order and violates the requested exception for canceling records.

### 6. Retry the complete Phase 1 decision

Phase 1 will use an outer bounded retry around this complete unit:

```text
read state -> classify lifecycle -> list incoming edges -> CAS pending|running to cancel-pending
```

A state conflict restarts from state classification and edge listing rather than allowing the generic state helper to retry only the final write. Existing `cancel-pending` is selected without rewriting or applying the active-record incoming-edge gate because it already represents a completed selection decision; this lets resumed planning finish idempotent outgoing-edge cleanup. Terminal records are skipped. Selected records then perform the existing idempotent pointer deletion, outgoing-edge deletion, and spawned traversal before Phase 2 begins.

Alternative: rely on the callee driver lock and retry only `state.json`. Rejected because lock lease expiry can transfer ownership, and the canonical cancellation contract requires fresh caller references after contention.

### 7. Make Phase 2 lifecycle filtering explicit

After acquiring the driver lock, Phase 2 classifies the latest lifecycle:

| Lifecycle | Action |
| --- | --- |
| `cancel-pending` | Respect `not_before`, invoke cancel adapter, persist retry state, or CAS to `canceled` |
| `canceled` | Return complete without warning or adapter invocation |
| `pending`, `running`, `succeeded`, `failed` | Log a warning with execution ID and lifecycle, then return complete without adapter invocation |

Warning-and-drop is diagnostic handling for contract drift, not a legal transition path. The hard transition guards are responsible for making `pending` or `running` after Phase 1 unreachable in normal operation.

Alternative: raise for lifecycle drift. Rejected because the requested operational behavior is to warn, avoid an invalid cancel request, and continue processing the remaining selected set.

## Risks / Trade-offs

- [Driver and state ownership are not atomically fenced across S3 objects] -> Verify owner on every attempt and rely on exact state CAS transitions; defer schema-level fencing.
- [Central field-diff validation may expose previously tolerated writes] -> Add an exhaustive writer and lifecycle matrix before converting orchestration paths.
- [Warning-and-drop can let cancellation return after detecting corrupted lifecycle membership] -> Emit explicit execution ID and lifecycle diagnostics; preserve this as an observable invariant breach while avoiding an invalid adapter call.
- [Registration cleanup can race another owner after lease expiry] -> Delete only matching pointers and unchanged execution snapshots still owned by the failed launch.
- [Local immutable DAG objects may be created before final result publication loses cancellation CAS] -> Treat them as unreachable storage artifacts; never publish them into canceled execution state.

## Migration Plan

No data migration is required. Deploy the state-authority helper and converted writers together because partial conversion would leave inconsistent enforcement. Existing records remain valid. Rollback restores the previous coordination behavior without converting stored data.
