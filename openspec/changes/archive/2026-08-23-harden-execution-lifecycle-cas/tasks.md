## 1. State Mutation Authority

- [x] 1.1 Add execution-coordination contract tests covering the complete lifecycle transition matrix, absorbing terminal lifecycles, lock-free result and lineage field allowlists, locked lifecycle and control fields, owner loss, and fresh-state reevaluation after CAS conflicts.
- [x] 1.2 Replace permissive semantic-state mutation with centralized changed-field, lifecycle-transition, and driver-owner validation while preserving the exact existing execution record schema.
- [x] 1.3 Convert activation, runtime result finalization, adapter-error publication, cancellation completion, and invalidation writers to the new locked or lock-free authority paths; ensure `mark_running` permits only `pending -> running`, result publication permits only `running`, and invalidation leaves `cancel-pending` state unchanged.

## 2. Child Registration and Completion

- [x] 2.1 Add deterministic contract tests proving caller cancellation either observes a registered spawned child or prevents adapter invocation, with attempted-edge removal and conditional cleanup limited to the fresh launch's matching pointer and unchanged owned split objects.
- [x] 2.2 Harden initial child registration so edge publication and a `running`-guarded spawned-summary CAS both complete before adapter invocation, while reused executions and shared content-addressed argument objects survive rejected registration.
- [x] 2.3 Add tests and implementation for lock-free normal terminal-child bookkeeping that CAS-moves `spawned_execution_ids -> child_execution_ids` for `running` and `cancel-pending` callers, persists before surfacing cancellation, and leaves retrying or canceled children spawned.

## 3. Cancellation Phases

- [x] 3.1 Add a parameterized Phase 1 lifecycle contract test proving only `pending` and `running` are rewritten to `cancel-pending`, existing `cancel-pending` is reconstructed without a write, and `succeeded`, `failed`, and `canceled` remain unchanged.
- [x] 3.2 Refactor Phase 1 contention handling to retry the complete lifecycle classification, incoming-edge listing, and guarded CAS decision while preserving complete planning before adapter work.
- [x] 3.3 Add Phase 2 lifecycle-matrix tests proving only `cancel-pending` invokes the cancel adapter, `canceled` drops silently, and `pending`, `running`, `succeeded`, and `failed` warn with execution identity and drop from the drive set.
- [x] 3.4 Implement Phase 2 lifecycle filtering and guard successful completion as exactly `cancel-pending -> canceled`, preserving persisted `not_before`, retry continuation, and bounded exhaustion behavior.
- [x] 3.5 Add public `Dml.runtime.cancel` contract coverage proving warning-and-drop statuses do not remain in the retry set while unsuccessful `cancel-pending` work still raises after exhaustion.
- [x] 3.6 Add deterministic integration coverage for activation, child registration/completion, and result publication racing synchronous runtime cancellation, including the shared-child preservation scenario from the flaky-CI investigation.

## 4. Documentation and Verification

- [x] 4.1 Update execution/runtime-state architecture and the flaky-CI investigation follow-up to document field authority, lifecycle transitions, registration ordering, and warning-and-drop Phase 2 diagnostics without changing persisted schemas.
- [x] 4.2 Run `openspec validate harden-execution-lifecycle-cas --strict`, targeted execution-coordination contract tests, and targeted lifecycle integration tests; resolve every failure.
- [x] 4.3 Run `uv run --dev --all-extras ruff check --fix .`, the repository typecheck command, `uv run --dev --all-extras pytest -m "not slow" .`, and the required slow lifecycle tests; confirm no schema, protocol, or public-signature changes appear in the final diff.
