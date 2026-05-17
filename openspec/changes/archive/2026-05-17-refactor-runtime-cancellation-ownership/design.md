## Context

The current runtime stores launch-time resume data, lifecycle state, dependency summaries, and cancellation metadata in one mutable execution object. `IndexOps.start_fn`, cancellation code, and execution-runtime update paths all write into that object, which makes ownership hard to reason about and forces `Dml.runtime.cancel(index_id)` to synchronously drive adapter cancellation work from a runtime that may not own the adapter context or permissions.

This change refactors the runtime around two durable records with explicit owners:

- `launch_state`: caller-owned launch/resume state protected by the cache-key lock
- `execution_record`: execution-runtime-owned lifecycle state protected by CAS

The naming is intentional. `launch_state` describes resumable launch state only. `execution_record` describes durable control-plane lifecycle state only. The change also renames cancellation lifecycle values so they no longer imply that backend shutdown has already completed.

## Goals / Non-Goals

**Goals:**
- separate launch/resume ownership from lifecycle/cancellation ownership
- make caller and execution-runtime write authority explicit
- keep `active/<cache_key>` semantics focused on whether a cached computation has a current execution attempt
- let user-triggered `dml.runtime.cancel(index_id)` work without an active caller execution context, using `config.user` as the cancellation requester
- preserve live caller edges for invalidation/orphan checks while preserving runtime-owned spawned execution lists for cancellation traversal
- make cancellation best-effort, bounded, and explicit about what the runtime guarantees

**Non-Goals:**
- guaranteeing that a `cancel-*` lifecycle means the backend process is already dead
- reconstructing historical runtimes solely to continue cancellation through terminal intermediates
- collapsing live caller edges and spawned execution lists into one graph structure
- removing cache-key locking from caller-owned launch-state transitions

## Decisions

### Decision: Split execution persistence into `launch_state` and `execution_record`

`launch_state` will contain:

- `execution_id`
- `cache_key`
- `resume_state`
- `created_at`

`execution_record` will contain:

- `execution_id`
- `cache_key`
- `lifecycle`
- `updated_at`
- `spawned_execution_ids`
- `cancellation_requested_by`

Rationale:
- `launch_state` is a caller-owned resumption handle tied to `active/<cache_key>`.
- `execution_record` is the execution runtime's durable control-plane state.
- separating them eliminates the current mixed ownership where caller runtimes, cancellation code, and execution runtimes all mutate the same object.

Alternatives considered:
- keep one monolithic execution object with stricter write discipline: rejected because it still couples unrelated invariants and leaves status/state confusion in place.
- keep resume state only in memory: rejected because multiprocessing and distributed runtimes need durable resume data.

### Decision: Keep cache-key locking for `launch_state` and use CAS for `execution_record`

The cache-key lock remains the serialization point for:

- creating or reusing `active/<cache_key>`
- reading and writing `launch_state`
- removing the active pointer during orphan-triggered cancellation transitions

`execution_record` remains independently CAS-updated with the latest ETag kept in memory by the owning runtime. If a CAS write fails due to ETag drift, the runtime rereads the record. It raises the cancellation exception only when the reread lifecycle is already a `cancel-*` value; otherwise it continues with the valid reread state.

Rationale:
- `launch_state` and `active/<cache_key>` are cross-object caller-owned invariants and need lock serialization.
- `execution_record` is mostly single-owner state and benefits from lock-free CAS updates.

Alternatives considered:
- use one lock for both objects: rejected because it would re-couple caller-owned and runtime-owned lifecycles.
- use CAS for `active/<cache_key>` transitions too: rejected because the active pointer and launch state need one caller-owned lock boundary anyway.

### Decision: Use `cancel-pending` and `cancel-detached` lifecycle values

The runtime will replace `cancel-requested` and `cancelled` with:

- `cancel-pending`: cancellation has been requested and must be observed by the execution runtime
- `cancel-detached`: the runtime completed its cancellation responsibilities and detached this execution from current ownership

`cancel-detached` does not mean the backend process is already dead. It means:

- the runtime removed `active/<cache_key>`
- future callers should create a new execution attempt instead of reusing this one
- any remaining backend shutdown is delegated to the adapter/executor contract

The `cancel-*` prefix is intentional so cancellation-aware write paths can cheaply identify cancellation lifecycles after an ETag reread.

Alternatives considered:
- keep `cancelled`: rejected because it implies a stronger guarantee than the runtime actually provides.
- use a non-prefixed detached term such as `detached`: rejected because the `cancel-*` prefix is useful in ETag-drift handling and is more obviously cancellation-related.

### Decision: Treat cancellation as out-of-band control-plane work

`dml.runtime.cancel(index_id)` is a user-triggered out-of-band workflow, not an in-band execution path. When called directly by a user, there is no active caller `execution_id`; in that case `cancellation_requested_by` is the configured user identity.

The cancellation flow is:

1. freeze the index so further mutation stops
2. read the root `execution_record.spawned_execution_ids`
3. remove caller-owned live caller edges for direct dependencies
4. for each callee that loses its last live caller:
   - acquire the callee cache-key lock
   - recheck that no live callers remain
   - confirm the callee is not terminal
   - remove `active/<cache_key>`
   - CAS `execution_record.lifecycle -> cancel-pending`
   - set `cancellation_requested_by`
   - release the lock
5. issue adapter cancellation fire-and-forget for queued cancellable executions
6. CAS those executions to `cancel-detached`
7. write cancellation tombstones with if-none-match protection
8. mark the root as `cancel-detached` and raise `CancelledExecutionError`

Rationale:
- the cancelling runtime is not guaranteed to have full adapter permissions or ownership.
- adapters are already required to process cancellation out of band and fully on their own side.
- removing the active pointer is the key signal that future callers must relaunch rather than resume.

Alternatives considered:
- keep synchronous cancellation until the full adapter chain confirms completion: rejected because it is chunky, permission-sensitive, and couples cancellation correctness to the caller runtime.
- defer all status mutation until adapters confirm shutdown: rejected because the runtime still needs to revoke current-execution ownership immediately.

### Decision: Keep two graph structures with different owners and meanings

`live-callers/<callee>/<caller>` remains caller-owned and represents current inbound callers. It is used for orphan detection and invalidation.

`execution_record.spawned_execution_ids` remains runtime-owned and represents the children started by that execution for cancellation traversal.

Rationale:
- these answer different questions and should not be conflated.
- live caller edges can shrink as callers cancel or disappear.
- spawned execution lists are historical execution summaries used for best-effort cancellation traversal.

Alternatives considered:
- make cancellation dependencies in-memory only: rejected because multiprocessing and distributed execution need durable traversal state.
- use live caller edges as the only dependency source: rejected because edge removal intentionally discards information that cancellation traversal still needs.

### Decision: Accept best-effort cancellation limits through terminal intermediates

The design accepts that `A -> B -> C` may leave `C` running if `B` has already gone terminal before `A` cancels and there is no practical runtime reconstruction path to continue propagation.

Rationale:
- reconstructing historical runtime context would require additional machinery and may be more expensive than leaving the descendant alone.
- the design optimizes for bounded, ownership-correct cancellation over perfect retrospective traversal.

Alternatives considered:
- recreate terminal intermediates solely to continue cancellation propagation: rejected for cost and complexity.
- maintain a stronger transitive cancellation graph with full replay metadata: rejected as too heavy for this refactor.

## Risks / Trade-offs

- [Cancellation semantics are weaker than the old name implied] -> rename the lifecycle values to `cancel-pending` and `cancel-detached`, and document that `cancel-detached` is a control-plane guarantee rather than proof of backend exit.
- [New callers may relaunch work while detached backend cleanup is still happening] -> make removal of `active/<cache_key>` an intentional part of the contract and require adapters to make their own cancellation side idempotent.
- [Best-effort traversal may miss descendants behind terminal intermediates] -> document this as an accepted limitation and cover it with explicit contract tests.
- [Two graph structures can drift if their roles are misunderstood] -> specify ownership and purpose separately in specs and tests.
- [CAS retries can race with non-cancellation updates] -> only treat ETag drift as terminal for cancellation when the reread lifecycle is already `cancel-*`; otherwise continue with the latest valid record.

## Migration Plan

1. Introduce the new record names, lifecycle names, and storage helpers behind the existing runtime flow.
2. Move `start_fn` launch/resume paths to `launch_state` while keeping active-pointer semantics intact.
3. Move lifecycle, dependency, and cancellation fields to `execution_record` and update executor envelopes.
4. Refactor `dml.runtime.cancel(index_id)` to the out-of-band orphan-detection and detach flow.
5. Update executor contracts and tests to return detached-style cancellation results.
6. Remove legacy monolithic execution-record assumptions once all contract tests pass.

Rollback strategy:
- revert the refactor as one change set before archive if contract coverage reveals incompatible runtime assumptions.
- no persistent data migration rollback is required yet because the change can land atomically with its new contract tests.

## Open Questions

- whether the adapter envelope field should remain named `execution_status` for compatibility while carrying the new lifecycle values
- whether `cancel-detached` should be persisted by the caller-side cancellation workflow, the execution runtime after observing `cancel-pending`, or both under tightly scoped CAS rules
- whether index-root `execution_record` entries should keep using the index id directly or be renamed separately in a later cleanup
