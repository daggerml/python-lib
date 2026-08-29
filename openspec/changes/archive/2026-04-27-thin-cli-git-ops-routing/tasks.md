## 1. DmlOps workflow surface

- [x] 1.1 Add git-like project workflow methods to `DmlOps` for `fetch`, `pull`, `push`, `checkout`, `merge`, and `revert` orchestration.
- [x] 1.2 Add clone workflow support in `DmlOps` that composes fetch + checkout while preserving current branch/tag semantics.
- [x] 1.3 Move or rehome CLI-local helper logic (project config loading, remote URI mapping, revision resolution helpers) into internal ops-owned code paths used by `DmlOps`.

## 2. Thin CLI refactor

- [x] 2.1 Update `src/daggerml/_cli/project.py` handlers so each command parses args and calls a single `DmlOps` method.
- [x] 2.2 Remove direct `CommitOps`/`RemoteOps` instantiation from `_cli` project handlers.
- [x] 2.3 Ensure `src/daggerml/_cli/base.py` command dispatch continues routing git-like operations via `DmlOps` without embedding business logic.

## 3. Verification and regression coverage

- [x] 3.1 Add/update CLI tests to assert git-like project commands remain thin delegates and preserve output/error contracts.
- [x] 3.2 Add/update internal ops tests for new `DmlOps` workflow methods, including checkout mode and merge/revert/pull/push orchestration paths.
- [x] 3.3 Run targeted test suites for CLI project commands and internal ops workflows; fix parity regressions before completion.
