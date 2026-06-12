## 1. Scope Guardrails

- [x] 1.1 Before editing, inspect the worktree and record any pre-existing non-contrib changes without modifying them.
- [x] 1.2 Confirm the intended implementation edit set is limited to `src/daggerml/contrib/**` and contrib-scoped tests/docs such as `tests/contrib/**` if needed.
- [x] 1.3 Do not modify `src/daggerml/api.py`, `src/daggerml/_core/dml.py`, `src/daggerml/_core/**`, `src/daggerml/__init__.py`, CLI code, storage code, or unrelated modules; if such a change seems necessary, stop implementation and report the blocker.

## 2. Public API Migration In Contrib

- [x] 2.1 Replace private `_core` imports in contrib with existing public `daggerml.api` or package-root imports where public exports already cover the required type or helper.
- [x] 2.2 Keep any remaining private `_core` imports only when no existing public API/export covers the required behavior, and document the reason locally in the implementation notes or final summary.
- [x] 2.3 Update contrib code that creates, loads, mutates, calls, or commits DAG values to use existing public DAG/session APIs where possible.

## 3. Runtime Envelope Reconciliation

- [x] 3.1 Update contrib adapter payload parsing to accept the current runtime envelope without requiring `argv_ptr`.
- [x] 3.2 Update runnable decoding in contrib adapters to handle the runnable dictionary shape emitted by the existing runtime implementation.
- [x] 3.3 Normalize contrib adapter/executor result handling inside contrib so returned payloads match the existing runtime caller's accepted schema.
- [x] 3.4 Update nested executor forwarding for Docker, SSH, Batch, Lambda, or other contrib transports so they preserve the current runtime envelope fields and do not require `argv_ptr`.

## 4. Worker DAG Creation

- [x] 4.1 Update script worker DAG creation to call `temporary(remote_root=...)` first, then `new(dml=temp_dml, cache_key=..., execution_id=...)`.
- [x] 4.2 Update CloudFormation executor worker DAG creation to use `cache_key` and `execution_id` instead of `argv_ptr`.
- [x] 4.3 Remove or ignore stale contrib-only `argv_ptr` plumbing where it is no longer needed by workers or nested adapters.
- [x] 4.4 Ensure workers continue to read invocation inputs through `dag.argv` after the migration.

## 5. Contrib Tests And Verification

- [x] 5.1 Update or add contrib-scoped tests for adapter payload parsing without `argv_ptr`.
- [x] 5.2 Update or add contrib-scoped tests for worker DAG creation from `cache_key` and `execution_id`.
- [x] 5.3 Run the relevant contrib-focused test set and record the command and outcome.
- [x] 5.4 Inspect the final diff and verify no forbidden non-contrib implementation files changed.
- [x] 5.5 If any non-contrib source change is present, revert only the implementer's own forbidden edit or stop and ask for a new proposal boundary; never include it in this change.
