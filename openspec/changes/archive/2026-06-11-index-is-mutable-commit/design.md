## Context

`Index` is the mutable repository state used while building a commit, but today it stores only `head` plus `dag`. Callers then bridge between `Index` and `Commit` conceptually even though the model says they are different object kinds. The next planned change, unborn HEAD, wants first-commit flows to work cleanly from either an existing commit or an explicit empty commit state.

## Goals / Non-Goals

**Goals:**
- Make `Index` a subtype of `Commit` in the internal object model.
- Keep the current runtime/history behavior unchanged in this stage.
- Simplify later unborn-HEAD work by letting index creation start from commit-shaped state.

**Non-Goals:**
- Changing user-visible `Dml` behavior.
- Introducing clone or unborn-HEAD semantics in this change.
- Reworking commit history rules.

## Decisions

- `Index` will inherit the commit fields and add only the mutable DAG-specific state needed during staging.
  Rationale: an index is a mutable commit, not a separate history primitive.
- `IndexOps.create(...)` will be refactored around commit-shaped input.
  Rationale: that keeps the change local to the runtime/history seam.
- This stage will preserve on-disk and workflow behavior unless a follow-up needs the new model.
  Rationale: the refactor should be verifiable before layering unborn-HEAD semantics on top.

## Risks / Trade-offs

- [Dataclass inheritance churn] -> Keep the first change narrowly scoped to `types.py` and `index.py` plus tests.
- [Accidental behavior drift in runtime commit flows] -> Preserve current contract tests and add model-focused coverage.
- [Over-specifying future unborn behavior too early] -> Defer unborn semantics to the next proposal.
