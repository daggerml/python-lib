## Context

See `proposal.md`. Cancellation already persists complete work as `cancel-pending`, so a second public drive mode duplicates state-derived behavior. The current sequential driver also terminalizes unsuccessful adapter outcomes and returns legacy summary buckets.

## Goals / Non-Goals

**Goals:**

- Make one call perform or resume the complete cancellation workflow.
- Make successful adapter cancellation the only path to `canceled`.
- Reduce cancellation code and public concepts substantially.

**Non-Goals:**

- Add another lifecycle or persistent retry counter.
- Preserve `mode` or the existing cancellation summary shape.
- Add background reconciliation or persistent attempt counters.

## Decisions

### One state-derived entry path

`cancel(execution, max_retries=3)` always derives the requester from persisted cancellation metadata when present, otherwise uses the current user, then runs planning and driving. Repeated calls naturally resume `cancel-pending` records. The `mode` branches are deleted rather than hidden behind compatibility code.

### Retry rounds are concurrent and deadline-aware

Phase 2 maintains only a set of remaining execution IDs. Each round submits every remaining ID concurrently, waits for every outcome, transitions confirmed successes to `canceled`, and carries only unsuccessful IDs into the next round. Before an invocation, its worker waits until the execution's persisted `driver.not_before`; a retry response persists adapter state and sets `not_before` from `retry_after_ms`. `max_retries` means retry rounds after the initial attempt, so the total maximum is `max_retries + 1` attempts per execution.

All selected executions run in the same round; no lineage wave ordering is retained. Phase 1 still completes before any adapter call, and executor cancellation remains idempotent.

### Success is strict and failure is resumable

Only adapter status `cancelled` confirms success. Retry, failure, protocol errors, and invocation exceptions are collected as unsuccessful outcomes without preventing other futures from settling. A worker acquires the execution coordination lock immediately before rereading invocation state, holds it across the adapter call and response persistence, and releases it in all outcomes. This prevents overlapping cancellation calls for one execution while preserving parallelism across executions.

After exhaustion, remaining records stay `cancel-pending` and `cancel()` raises `DmlRepoError` identifying them. A later call receives a fresh retry budget. Successful cancellation returns `None`; `RuntimeCancelSummary` and its legacy buckets are removed.

### Keep concurrency local and minimal

Use the standard-library futures executor directly in Phase 2. Do not add a scheduler, persistent attempt model, response wrapper hierarchy, or compatibility adapter.

## Risks / Trade-offs

- [Concurrent parent and child teardown may overlap] -> Require idempotent, order-independent executor cancellation; Phase 1 completes discovery before teardown.
- [A long adapter call holds one execution lock] -> Keep calls parallel across executions and guarantee lock release with structured cleanup.
- [Nested work may contend on a held parent lock] -> Nested cancellation targets distinct child execution locks; executors must not recursively acquire their own execution lock.
- [Breaking callers using `mode` or summaries] -> Document the direct replacement and intentionally provide no compatibility path.
- [One future may raise] -> Collect each future independently so unrelated executions still make progress.

## Migration Plan

Update internal and external callers to remove `mode` and optionally pass `max_retries`. No persisted-state migration is required; existing `cancel-pending` records are resumed by the unified path. Rollback requires callers to restore the old API together with the old implementation.
