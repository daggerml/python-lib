## Context

The runtime already treats `cache_key` as the computation identity for execution startup and resume. `start_fn` acquires the coordination lock by `cache_key`, reads the active execution pointer for that cache key, and either launches or resumes a single in-flight execution attempt. Cancellation diverges from that model today by locking the candidate execution id directly, even though the execution record already stores the canonical `cache_key` for the work being cancelled.

The requested change keeps index-rooted cancellation as the entry point, but makes its remote coordination consistent with normal execution. The synthetic index execution record remains the root of the cancellation graph, and its recorded execution-id dependencies continue to drive cancellation planning.

This change also redefines cancellation from an incremental bounded sweep into a full rooted-graph pass followed by a retryable cancellation loop. `Dml.runtime.cancel` owns that loop, emits diagnostics about each pass, and returns a structured statistics object. Each cancellation attempt must traverse the full execution graph reachable from the index, collect the full caller-callee graph, derive the rooted candidate executions, and then repeatedly attempt short-lived per-candidate cancellation steps until the rooted candidate set is exhausted.

## Goals / Non-Goals

**Goals:**
- Make cancellation acquire the same remote coordination lock identity as launch and resume: `cache_key`.
- Ensure the synthetic index-root execution transitions to `cancel-requested` before any graph work begins and to `cancelled` only after the rooted graph is fully cancelled.
- Traverse the full rooted execution graph on every cancellation attempt and derive rooted cancellation candidates from that graph.
- Evaluate cancellation eligibility against the global caller set stored in S3, not only the rooted traversal graph.
- Use short-lived per-candidate cache-key locks inside a retryable loop rather than one long-lived batch lock.
- Preserve rooted cancellation planning by execution id while resolving lock identity from each candidate execution record.
- Reduce races between cancellation and concurrent `start_fn` activity for the same computation.

**Non-Goals:**
- Redesign the execution graph model, dependency recording, or invalidation flow.
- Change adapter payload shape beyond the existing `execution_status = "cancel-requested"` update path.
- Introduce new persistent coordination objects beyond the existing execution record, active pointer, and lock objects.

## Decisions

### 1. Cancellation will resolve `cache_key` from the execution record before locking

Cancellation starts from execution ids because dependency edges and rooted planning are keyed by execution id. The lock, however, protects the computation identity, not the lineage identity. The cancellation path should therefore read `exec/state/<execution_id>.json`, obtain `cache_key`, construct `ExecutionState(cache_key, ...)`, and acquire that lock before re-reading and mutating the candidate execution record.

Alternative considered: keep locking by `execution_id` and document cancellation as a special case. Rejected because it preserves the current mismatch and still allows cancellation to coordinate independently from launch/resume for the same computation.

### 2. The index root record will be marked `cancel-requested` before any graph work begins

The synthetic index execution record represents the rooted cancellation request itself. Marking it `cancel-requested` first makes the remote state reflect that cancellation is in progress before traversal, caller counting, or adapter cancellation begins. This is also required for correct active-caller counting: descendants must observe that their rooted caller is already in `cancel-requested` rather than still appearing live. The index root should only move to `cancelled` after the entire rooted graph has been cancelled without error.

Alternative considered: leave the index root as `running` until the end and mark only real executions. Rejected because it hides cancellation progress from readers and leaves the root record inconsistent with the requested operation.

### 3. Cancellation will use a two-phase algorithm: graph discovery, then retryable cancellation

Each cancellation attempt should first walk the full execution graph rooted at the index's direct dependencies and collect `graph := {(caller, callee), ...}`. From that graph, the runtime derives `candidate_set := {callee}` and seeds `own_executions := candidate_set.copy()`. Those sets define the rooted cancellation universe for this pass.

The runtime should then run a loop over the remaining `candidate_set`. In parallel across the current candidate set, it should attempt to acquire the candidate's cache-key lock, return immediately on lock contention, recompute the candidate's active callers from the global reverse-edge set in S3, and decide one of three outcomes: keep retrying, drop the candidate from `candidate_set`, or drop it from both `candidate_set` and `own_executions`. This makes cancellation retryable, keeps lock hold times short, and allows ownership to shrink as externally referenced executions are identified.

Alternative considered: continue the current incremental work-queue sweep that interleaves discovery and cancellation decisions. Rejected because it couples graph traversal to transient node status, makes the meaning of `cancelled` leak into graph completion, and makes active-caller results depend on discovery order.

### 4. Caller ownership must use the global reverse-edge set

`callers(c)` must mean all recorded callers of `c` from the reverse-edge records in S3, not only callers discovered while traversing the cancelled index's rooted graph. If index `A` and index `B` both call `X`, then cancelling `A` must not cancel `X`. If `A` calls `X` and `Y`, and `Y` calls `X`, then `cancel(A)` may cancel `X` when `callers(X) = {A, Y}` because every caller belongs to the cancelled index's rooted ownership set.

Alternative considered: infer ownership only from the rooted traversal graph. Rejected because it can wrongly cancel shared executions that still have callers from unrelated indexes or execution subgraphs.

### 5. Candidate processing is loop-based and short-lock

For each candidate in the current loop iteration, `Dml.runtime.cancel` should attempt to lock, inspect, act, and unlock quickly. The worker returns `None` when it could not make progress, `-1` when the candidate is discovered not to be fully owned by the cancelled index, and `+1` when the candidate reached per-execution `cancelled`. The outer loop removes `+1` candidates from `candidate_set` and removes `-1` candidates from both `candidate_set` and `own_executions`, logs iteration diagnostics, and repeats until `candidate_set` is empty.

Alternative considered: hold one long-lived lock batch across the whole owned subgraph. Rejected because it increases contention and makes retries more expensive.

### 6. `Dml.runtime.cancel` will return structured cancellation statistics

`Dml.runtime.cancel` should return a deterministic summary object rather than ad hoc diagnostics. At minimum, that object should report the target `index_id`, total loop `iterations`, the size of the rooted `candidate_set` discovered during graph traversal, how many candidates were retained in `own_executions`, how many per-execution cancellations completed, how many candidates were removed because they had external active callers or were otherwise not eligible, and how many lock-contention retries occurred. This gives operators a stable surface for observability and tests a precise contract to assert.

Alternative considered: return only a boolean or log-only diagnostics. Rejected because it hides useful convergence information and makes cancellation behavior harder to validate automatically.

### 7. Candidate locking will use a two-step read: resolve, then lock, then re-read

The cancellation path should first read the candidate execution record without a lock to discover `cache_key`, then acquire the cache-key lock, then re-read the same execution record while holding that lock before making cancellation decisions. This preserves correctness if the record changed between the first read and lock acquisition.

Alternative considered: add a new direct mapping from execution id to lock key. Rejected because the execution record already provides the needed mapping and is the authoritative source.

### 8. `cancelled` is a per-execution cleanup-complete status, not a graph-complete status

For a non-index execution, `status = "cancelled"` means that execution's cleanup is complete and the index-cancellation runtime does not need to invoke that execution's adapter chain again. It does not mean traversal can stop at that node or that the rooted graph has been fully cancelled. The index remains responsible for traversing the full graph on every cancellation attempt until the index itself can be marked `cancelled`.

Alternative considered: treat a `cancelled` execution as a graph-terminal pruning point. Rejected because descendant executions may still require cancellation work even when a parent execution's own cleanup is done.

### 9. Completion cleanup remains gated on successful full-graph completion

The temporary cancelled-index marker should still be removed only after the rooted graph has been fully processed and the index-cancellation runtime can conclude the graph is cancelled. If the cancellation pass fails, the marker should remain so cancellation can be retried, while the index root record remains `cancel-requested` rather than being advanced to `cancelled` prematurely.

### 10. Terminal `cancelled` state is owned by the index-cancellation runtime

Neither the remote execution runtime nor any single adapter in the adapter chain can authoritatively decide that an execution is fully `cancelled`. Each adapter layer may own cleanup for its own state, and a callee adapter does not know whether it is the leaf, the root, or an intermediate step in the chain. Because of that, a remote cancel update can report that one adapter handled its own cancellation work, but the index-cancellation runtime must continue driving the full adapter chain until every participating layer has had a chance to process cancellation. Only the index-cancellation runtime should persist terminal `cancelled` state, and only after that full chain completes.

Alternative considered: allow the first remote adapter or executor that returns `cancelled` to finalize the whole execution. Rejected because that can strand cleanup work in outer or sibling adapter layers that have not yet run their own cancellation handling.

### 11. Unreachable remote-only adapter chains remain a known limitation

Today, `Dml.runtime.cancel` can only actively drive cancellation work for adapter chains that are reachable from the index runtime process. If execution `X` was started by the index runtime but `X` then delegated to execution `Y` through bespoke adapters that only exist on some remote machine, the index runtime may be able to mark `Y` as `cancel-requested` but may not be able to invoke the adapter path that would actually finish cancelling `Y`. In that case, the retry loop can continue indefinitely until the user interrupts it, typically with `Ctrl+C`.

This is an accepted limitation for now. The current design relies on `cancel-requested` as the durable propagation signal so future remote executors and bespoke adapter stacks can learn to observe that state and complete their own cancellation work without requiring the index runtime to invoke them directly.

Alternative considered: block this change until every remote-only executor path can autonomously react to `cancel-requested`. Rejected because the coordination and state-model improvements are still valuable now, even though some remote execution environments will remain partially manual.

## Risks / Trade-offs

- [Extra record read before locking] -> Mitigation: cancellation already depends on execution-record reads; add tests that cover the resolve-lock-reread sequence.
- [Longer overall cancellation pass due to full-graph traversal] -> Mitigation: separate discovery from retryable cancellation processing, reuse stored dependency edges, and keep per-node cancellation work bounded within short lock windows.
- [Global caller reads may be stale relative to lock acquisition] -> Mitigation: recompute active callers under lock before acting and retry on later loop iterations.
- [Lock contention causes slow convergence] -> Mitigation: make each worker return `None` on lock failure and let `Dml.runtime.cancel` retry without holding unrelated locks; document contention as a sharp edge and keep locks intentionally short-lived.
- [Synthetic index root may remain `cancel-requested` after a failed sweep] -> Mitigation: treat that state as a visible retryable cancellation-in-progress marker and keep the cancelled-index pointer for retried cleanup.
- [Spec drift between launch/resume and cancellation locking semantics] -> Mitigation: update both cancellation and runtime execution-record capabilities in the same change.
- [A remote adapter reports `cancelled` before outer adapter cleanup has run] -> Mitigation: keep terminal `cancelled` state owned by the index-cancellation runtime and continue driving cancellation through the full adapter chain before finalizing state.
- [Per-execution `cancelled` status is mistaken for graph completion] -> Mitigation: define `cancelled` explicitly as a per-execution status and make index `cancelled` contingent on full rooted-graph completion.
- [Remote-only bespoke adapter chains are unreachable from the index runtime] -> Mitigation: document this as a sharp edge, persist `cancel-requested` for those executions, and expect user interruption until downstream runtimes adopt autonomous `cancel-requested` handling.

## Migration Plan

1. Update the cancellation contract and runtime locking contract.
2. Change the cancellation implementation to traverse the full rooted graph, compute `graph`, seed `candidate_set` and `own_executions`, and drive a retryable cancellation loop using global caller ownership and short-lived cache-key locks.
3. Update contract tests for full-graph traversal, global caller ownership, loop outcomes (`None`, `-1`, `+1`), per-execution `cancelled` semantics, and index-root status transitions.
4. Rollback, if needed, is a code revert; no persisted schema migration is required.

## Open Questions

- None. Lock contention and unreachable remote-only adapter chains remain documented sharp edges, but both are accepted for this design phase.
