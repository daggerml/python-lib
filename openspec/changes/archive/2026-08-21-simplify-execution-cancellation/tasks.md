## 1. Lifecycle Model

- [x] 1.1 Replace `cancel-requested` and `cancel-ready` with `cancel-pending` in execution lifecycle types, record validation, graph/cache descriptions, CLI rendering, and test fixtures; keep the existing runtime cancellation summary shape with an always-empty `timeout` list.
- [x] 1.2 Update activation, mutation, result publication, child spawning, and supervisor lifecycle guards so `cancel-pending` blocks all further index/execution mutation and raises the cancellation error expected at those boundaries.
- [x] 1.3 Add or update fast contract tests proving `cancel-pending` is accepted, the removed values are rejected, and every mutation boundary refuses work after selection.

## 2. Phase 1 Selection And Caller Coordination

- [x] 2.1 Serialize caller-edge registration with the callee execution lock, revalidate the callee lifecycle before adapter invocation, and remove incomplete caller edges when registration loses to `cancel-pending` or otherwise fails.
- [x] 2.2 Replace the existing cancellation planner with an iterative Phase 1 that returns an ordered selected set, reconstructs already-`cancel-pending` records, skips terminal records without error, defers rather than permanently deduplicates referenced candidates, retries CAS conflicts from fresh lifecycle and caller state, and performs idempotent cache-pointer and outgoing-edge cleanup only for selected executions.
- [x] 2.3 Add execution-coordination contract tests for terminal completion races, valid external callers, registration-versus-selection races, shared diamond descendants, duplicate/cyclic lineage, cache-pointer rebinding, CAS retries, retry exhaustion, and recovery after interruption between the `cancel-pending` write and graph cleanup.

## 3. Phase 2 Adapter Cancellation

- [x] 3.1 Replace recursive `cancel-ready` driving and timeout handling with Phase 2 processing of the completed Phase 1 set in reverse selection order; invoke each applicable persisted adapter target and CAS lifecycle directly from `cancel-pending` to `canceled`, while accepting terminal races and retrying conflicts from fresh state.
- [x] 3.2 Make `full` mode run Phase 1 then Phase 2, make `drive` mode reconstruct and resume persisted `cancel-pending` work through the same engine, and make direct cancellation of an already-terminal execution return an empty successful result instead of `BadExecutionStatusError`.
- [x] 3.3 Add contract and integration coverage proving no adapter runs before Phase 1 completes, descendants are processed before selected callers, cacheless roots become canceled without dispatch, interrupted and concurrent drivers converge, adapter exceptions leave retryable `cancel-pending` state, repeated adapter cancellation is safe, and `timeout` remains present and empty.

## 4. Remove Obsolete Protocol

- [x] 4.1 Remove the readiness timeout constant, `cancel-ready` transitions, recursive readiness checks, and tests that encode the old distributed handoff, without adding lifecycle compatibility branches.
- [x] 4.2 Update runtime, execution/cache, cancellation-guide, architecture, adapter-operation, and executor-lifecycle documentation to describe complete Phase 1 selection, `cancel-pending`, direct Phase 2 cleanup, terminal no-op behavior, and CAS-owned lifecycle persistence.

## 5. Verification

- [x] 5.1 Run targeted execution coordination, runtime cancellation gate, supervisor, and cancellation integration tests with `uv run --dev --all-extras pytest <paths>` and confirm all new race, recovery, graph, and lifecycle assertions pass.
- [x] 5.2 Run `uv run --dev --all-extras ruff check --fix .`, the repository typecheck required by the finish-check skill, and `uv run --dev --all-extras pytest -m "not slow" .`; resolve all failures attributable to this change.
- [x] 5.3 Run `openspec validate simplify-execution-cancellation --strict` and confirm the proposal, design, five capability deltas, tasks, implementation, and human-facing documentation remain consistent.
