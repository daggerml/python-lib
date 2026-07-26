## Context

`IndexOps` currently mixes two different mutation guards: execution-aware `create(...)` reads S3-backed execution state directly, while the other mutators gate on local `Index.lifecycle` inside LMDB transactions and sometimes drive cancellation as a side effect after catching `CancellationError`. The runtime therefore has two authorities for whether a write is legal, and neither is reusable from the `Dml` boundary that owns the full runtime orchestration path.

The desired refactor makes execution records in S3 the sole authority for mutation eligibility. Mutation methods will instantiate one `ExecutionState`, ask it to classify the target execution lifecycle for either activation or normal mutation, and then continue using that same state object as needed.

## Goals / Non-Goals

**Goals:**

- Centralize runtime mutation eligibility in one public `ExecutionState` method.
- Make activation and normal mutation modes share one lifecycle-classification table.
- Replace broad lifecycle failures with a typed hierarchy: `BadExecutionStatusError` and `CanceledExecutionError`.
- Remove local `Index.lifecycle` as a second mutation authority.
- Prepare runtime mutators for a `_core/dml.py` retry decorator that can replay the full orchestration path.

**Non-Goals:**

- Changing read-only runtime inspection methods such as `describe`, `list`, `read_execution_record`, or `describe_graph`.
- Redefining the execution lifecycle vocabulary beyond the existing states.
- Changing adapter cancel response contracts or executor-specific cancellation behavior.

## Decisions

### ExecutionState owns the public mutation guard

`ExecutionState` will expose `require_mutation(execution_id, db, *, mode="activation" | "mutation")` as the public lifecycle guard for runtime writes.

- `mode="activation"` allows only `pending`.
- `mode="mutation"` allows only `running`.
- `cancel-pending` triggers `cancel(execution_id, None, db, mode="drive")` before raising.
- `cancel-ready` and `canceled` raise immediately.
- All other non-allowed states raise a wrong-status error.

This keeps the classification table and the `cancel-pending` side effect in one place instead of scattering them across `IndexOps` methods.

Alternative considered: separate `require_activation(...)` and `require_mutation(...)` methods. Rejected because the only difference is the allowed lifecycle predicate, while the cancellation behavior and error mapping are otherwise identical.

### S3 execution records are the sole mutation authority

Mutation eligibility will be derived from execution lifecycle state in S3 rather than from a local `Index.lifecycle` field. Local index reads inside transactions still matter for structural validation, but they will no longer decide whether the execution is mutable.

Alternative considered: keep `Index.lifecycle` as a local mirror or fallback. Rejected because it preserves split-brain semantics and forces every mutator to reconcile LMDB state with the execution record.

### Cancel-family failures are a typed subset of wrong-status failures

The runtime will introduce:

- `BadExecutionStatusError`
- `CanceledExecutionError(BadExecutionStatusError)`

This lets callers catch all lifecycle gating failures broadly while still distinguishing the cancel-family states when needed.

Alternative considered: continue using `DmlRepoError` or `CancellationError` for all gating failures. Rejected because wrong-status and canceled-state failures are semantically narrower and now form an explicit public mutation guard contract.

### DML owns whole-operation retries

The retry wrapper belongs in `_core/dml.py`, not on `IndexOps`, because runtime operations such as `runtime.create(...)` and `runtime.commit(...)` span more than one lower-level call. Retrying only the inner index method would miss `HEAD` reads, branch updates, and any other orchestration work performed before or after the index operation.

The first implementation should keep the retry scope narrow and start with the existing DB-layer transient failures that already imply replayable work, then decorate runtime mutation entrypoints in `Dml`.

Alternative considered: decorate `IndexOps` methods directly. Rejected because that would not retry the full caller-visible operation boundary.

## Risks / Trade-offs

- State classification now depends on remote reads before each write path -> Mitigation: keep one shared helper and reuse the same `ExecutionState` object within a workflow.
- Removing `Index.lifecycle` eliminates a local tombstone that could aid debugging -> Mitigation: rely on execution-record inspection and execution graph tooling as the canonical source of truth.
- DML-level retries can replay larger units of work -> Mitigation: restrict retries to bounded, known-replayable failures and keep the decorated surface to runtime mutators.

## Migration Plan

1. Add the new exception types and `ExecutionState.require_mutation(...)`.
2. Move `IndexOps.create(...)` activation gating and all mutating `IndexOps` methods onto the new guard.
3. Remove `Index.lifecycle` checks and storage.
4. Add the DML retry decorator and apply it to runtime mutation methods.
5. Update contract tests to cover activation, mutation, cancel-pending drive, and typed status failures.

## Open Questions

- The retry decorator should start with existing replayable DB/env failures, but the exact default attempt count and backoff policy still need to be fixed during implementation.
