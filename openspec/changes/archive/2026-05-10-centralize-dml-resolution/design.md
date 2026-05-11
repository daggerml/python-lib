## Context

`src/daggerml/_internal/dml_resolution.py` already resolves revisions and basic DAG selectors, but `src/daggerml/_internal/dml.py` still contains node-specific lookup rules and wrapper methods that assemble resolution payloads. That split leaves selector parsing, ambiguity checks, and canonicalization spread across two modules even though they are part of the same concern: turning user-facing selectors into stable internal refs.

This change needs a design document because the behavior crosses commit, DAG, and node lookup paths and should produce one shared resolution contract that DML callers can rely on.

## Goals / Non-Goals

**Goals:**
- Make `dml_resolution.py` the single home for fuzzy selector resolution logic used by DML.
- Provide explicit helpers for commit, DAG, and node resolution that always return canonical `Ref` objects for resolved entities.
- Preserve the existing ergonomic input forms where they are unambiguous, including direct refs, raw object ids in supported formats, and named lookups.
- Move `dml.py` to a thin orchestration role that calls shared resolution helpers instead of re-implementing selector parsing.

**Non-Goals:**
- Redesign the CLI or user-facing output payload shapes beyond what is required to reflect the new canonical resolution behavior.
- Change storage layouts, ref encodings, or DAG/node persistence semantics.
- Broaden selector syntax beyond commit, DAG, and node resolution needs covered by the current DML surface.

## Decisions

### Introduce shared resolution helpers in `dml_resolution.py`

`dml_resolution.py` will own helper functions for revision, DAG, and node resolution. Each helper will accept the operation dependencies it needs (`commit_ops`, `dag_ops`, `head_ops`, `project_dir`) and return resolved refs plus any minimal metadata needed by callers.

Rationale: this keeps resolution behavior centralized without coupling the module to `Dml` instance internals.

Alternative considered: keep resolution wrappers in `dml.py` and only move small parsing helpers. Rejected because it would leave behavior split across modules and preserve the current drift risk.

### Make DAG and node resolution canonicalize to `Ref`

DAG resolution will continue to accept either an explicit `dag:` ref or a DAG name resolved through a commit selector, but the resolved object returned to callers will always be a `Ref`. Node resolution will similarly return a node `Ref` whether the input was already a ref, a node-id style selector, or a name looked up through a DAG.

Rationale: callers should not need to track whether a selector was direct or fuzzy after resolution succeeds.

Alternative considered: keep mixed return shapes such as `(optional_ref, name)` and let each caller finish the lookup. Rejected because it pushes ambiguity handling back out to callers.

### Define ambiguity handling around named node lookups

Node resolution will recognize three cases:
- Direct node refs, which resolve immediately.
- Node-id style selectors such as `node-literal:abc123`, which are interpreted as canonical node refs if valid.
- Named node selectors, which require DAG context only when the selector is not already a direct ref.

If the node selector is a name and multiple DAGs could satisfy the lookup without an explicit DAG selector, resolution must fail with a clear repository error asking for DAG disambiguation instead of guessing.

Rationale: this preserves convenience for unambiguous selectors while making ambiguity explicit.

Alternative considered: always require `dag_selector` for name-based node lookup. Rejected because it would remove an intended ergonomic path and is stricter than the requested behavior.

### Keep `dml.py` as an orchestration layer only

`dml.py` will stop parsing node selector strings or deciding when a DAG selector is mandatory. It will call `dml_resolution.py` helpers, then use the returned refs to build payloads and invoke ops methods.

Rationale: `dml.py` should coordinate operations, not own selector semantics.

Alternative considered: duplicate small guards in `dml.py` for readability. Rejected because “small” resolution checks tend to grow and recreate the current split.

## Risks / Trade-offs

- [Behavior drift in edge-case selectors] -> Add or update focused tests for direct refs, raw ids, named selectors, and ambiguous node lookups.
- [Resolution helpers may need more dependencies passed in] -> Prefer a small number of explicit helper parameters over reaching back into `Dml` state.
- [Existing callers may assume mixed return shapes] -> Update all DML internal call sites in the same change so the new contract is applied consistently.

## Migration Plan

1. Add the new shared resolution helpers in `dml_resolution.py`.
2. Update `dml.py` to delegate commit, DAG, and node selector handling to those helpers.
3. Adjust or add tests for canonical ref returns and ambiguity errors.
4. Run the relevant test suite for DML and selector-related behavior.

Rollback is straightforward: the change is internal-only and can be reverted by restoring the previous helper split if regressions appear.

## Open Questions

- Whether node-name lookup without an explicit DAG selector should search only the selected commit’s named DAG map or also support broader repository-wide fallback. The current implementation intent suggests commit-scoped lookup, and this change assumes that narrower rule unless tests or existing behavior require otherwise.
