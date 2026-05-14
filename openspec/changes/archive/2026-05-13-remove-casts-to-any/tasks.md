## 1. Contrib Authoring Typing

- [x] 1.1 Remove `cast(..., Any)` from `src/daggerml/contrib/api.py` while keeping the current dagclass, run, and funkify behavior unchanged.
- [x] 1.2 Make only the smallest local follow-up edits needed if raw cast removal exposes a concrete type or test failure in contrib authoring paths.

## 2. Execution Record Typing

- [x] 2.1 Remove `cast(..., Any)` from execution-record construction in `src/daggerml/_internal/ops/index.py`.
- [x] 2.2 Remove `cast(..., Any)` from execution-record merge logic in `src/daggerml/_internal/exec_state.py`, making only minimal local fixes if required.

## 3. Test Cleanup And Verification

- [x] 3.1 Remove `cast(..., Any)` usage from contrib integration tests by passing concrete values directly and making only minimal local fixes if required.
- [x] 3.2 Remove `cast(..., Any)` usage from config contract tests while preserving the legacy-alias rejection assertion.
- [x] 3.3 Run the focused test coverage for contrib integration and config/internal execution-state paths, and confirm no `cast(..., Any)` usages remain.
