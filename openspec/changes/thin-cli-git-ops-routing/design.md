## Context

Current git-like project commands are implemented partly in `src/daggerml/_cli/project.py` and partly in internal ops classes. This splits orchestration concerns across layers, duplicates repository/remote wiring logic, and makes behavior-level tests depend on CLI internals instead of stable internal interfaces.

The target architecture in repository docs is a thin CLI surface over internal operations (`DmlOps` facade + specialized ops modules). This change applies that layering rule consistently to git-like project commands.

## Goals / Non-Goals

**Goals:**
- Keep `src/daggerml/_cli/` handlers focused on parsing/validation and single-call delegation.
- Move git-like project operation orchestration into `DmlOps` methods that coordinate existing `CommitOps` and `RemoteOps` behavior.
- Preserve current command semantics and JSON error behavior while changing ownership boundaries.
- Make tests assert behavior through `DmlOps` boundaries instead of CLI-private helper functions.

**Non-Goals:**
- Redefine merge/revert/fetch protocol semantics already owned by `CommitOps`/`RemoteOps`.
- Introduce new user-facing command flags or alter command output shapes.
- Redesign remote URI formats or project config schema.

## Decisions

### 1) Add git-like project entrypoints on `DmlOps`
`DmlOps` will expose explicit methods for project-level workflows (`fetch`, `pull`, `push`, `checkout`, `merge`, `revert`, and clone composition support). These methods will own cross-subsystem coordination and shared helper logic currently in CLI utilities.

Alternative considered: move logic directly into `CommitOps`/`RemoteOps` only. Rejected because these workflows span both subsystems plus project config concerns; `DmlOps` is the existing facade intended for orchestration.

### 2) Keep CLI handlers as strict adapters
`src/daggerml/_cli/project.py` command functions remain responsible for argparse-facing input parsing only, then call one `DmlOps` method and return serialized results.

Alternative considered: leave mixed helper functions in CLI for practicality. Rejected because it weakens the thin-CLI contract and increases duplication risk.

### 3) Preserve existing command contracts while moving ownership
The refactor will keep observable command behavior stable (success payload fields, detached/attached checkout reporting, and failure patterns). Any behavior changes must be explicitly captured in specs and tests.

Alternative considered: take opportunity to simplify command outputs. Rejected for this change to minimize migration risk.

## Risks / Trade-offs

- [Risk] Refactor unintentionally changes edge-case behavior for revision resolution or checkout mode detection.
  → Mitigation: add parity tests for key scenarios (branch checkout, detached checkout, unresolved revision, clone branch/tag).

- [Risk] `DmlOps` grows too broad if orchestration methods become overly large.
  → Mitigation: keep `DmlOps` methods thin coordinators that call focused private helpers or existing ops methods.

- [Risk] CLI tests may become brittle during transition.
  → Mitigation: update tests to assert delegation boundaries and user-visible outputs, not helper implementation details.

## Migration Plan

1. Add new `DmlOps` project-operation methods and supporting internal helpers.
2. Update CLI project handlers to call those methods directly.
3. Port/add tests to validate both delegation and user-visible parity.
4. Run targeted CLI and internal ops test suites; fix regressions before merge.

Rollback: revert the `DmlOps` delegation changes and restore prior CLI helper ownership if parity breaks cannot be resolved in-scope.

## Open Questions

- Whether clone-specific setup concerns (filesystem/project-layout steps) should eventually be split into a dedicated internal project service versus remaining as a `DmlOps` method.
