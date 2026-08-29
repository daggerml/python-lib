## Context

`src/daggerml/_internal/dml.py` currently exposes a clean public calling surface, but its implementation is organized around a large private helper-method layer on `Dml` itself. Namespace classes mostly route through `._dml`, yet `_DagNamespace` still carries a private helper method of its own. The result is that callers see one orchestration boundary while the implementation relies on a second, informal instance-level private API inside the same file.

The requested change is narrower than a public API redesign. `Dml` may continue to keep `_context` and `_tempdirs` as private state, and namespace instances may continue to keep `._dml`. The goal is to move helper behavior out of private methods and into file-level functions so the `Dml` instance boundary is simpler and more explicit.

## Goals / Non-Goals

**Goals:**
- Remove private helper methods from `Dml`.
- Remove private helper methods and extra private attrs from namespace classes, leaving only `._dml` on namespaces.
- Re-home helper behavior in module-level functions within `src/daggerml/_internal/dml.py`.
- Preserve existing caller-facing `Dml`, `dml.dag`, `dml.runtime`, `dml.admin`, and `dml.config` behavior.
- Keep the change mostly mechanical so test updates can focus on structure rather than semantics.

**Non-Goals:**
- Renaming `Dml._context` or `Dml._tempdirs`.
- Redesigning `Dml` public methods, namespace names, payload formats, or revision grammar.
- Moving helper logic into other modules unless an existing imported helper already owns that concern.
- Performing unrelated cleanup in `daggerml.api`, CLI modules, or lower-level ops classes.

## Decisions

### Decision: Replace `Dml` private helper methods with module-level helper functions
Helper behaviors currently implemented as `Dml._...` methods will move to top-level functions in `dml.py` that accept the `Dml` instance explicitly when needed.

This includes:

- ops acquisition and dispatch helpers
- payload shaping helpers
- revision-resolution wrappers that bind the current runtime context
- remote/S3 helper setup

Rationale:

- keeps `Dml` itself limited to state and public workflows
- makes helper dependencies explicit through function arguments instead of implicit `self`
- matches the requested architectural rule without changing behavior ownership

Alternatives considered:

- Keep private methods and only rename them: rejected because it does not change the architectural shape.
- Move helpers into a new module: rejected because the request specifically prefers functions defined in `dml.py` and the current helpers are tightly local to this file.

### Decision: Keep private state exceptions exactly where requested
`Dml` will continue to store `_context` and `_tempdirs`, and namespace dataclasses will continue to store `._dml`. No other private attrs will be added to those objects.

Rationale:

- respects the explicit boundary the change is meant to enforce
- avoids churn in tests and call sites that already inspect `_context`

Alternatives considered:

- Make `context`/`tempdirs` public: rejected because the user explicitly narrowed the change away from that.

### Decision: Namespace methods delegate only through module-level helpers plus `._dml`
Namespace methods will stop calling private `Dml` methods. Instead, they will call file-level helpers such as revision resolvers, ops accessors, payload builders, or simple utility functions.

Rationale:

- removes the second-layer private API from `Dml`
- keeps namespace objects thin and declarative

Alternatives considered:

- Let namespace methods inline all helper logic: rejected because it would duplicate orchestration details and make the file harder to maintain.

### Decision: Keep `_OpsProxy` as an implementation detail only if it remains the smallest clean mechanism
The contract for this change is about `Dml` and namespace private methods/attrs, not every private symbol in the file. If an `_OpsProxy`-style helper remains the smallest way to keep ops lifetimes and call syntax stable, it may stay as a file-local implementation detail. If direct helper functions are clearer, it may be removed during implementation.

Rationale:

- preserves flexibility during the refactor
- keeps the spec focused on the actual architectural boundary the user cares about

Alternatives considered:

- Require `_OpsProxy` removal in the design: rejected because it is not necessary to satisfy the requested boundary rule.

## Risks / Trade-offs

- [Risk] Mechanical call rewrites accidentally change which ops helper is used or when an ops handle is opened/closed -> Mitigation: preserve existing helper responsibilities one-for-one first, then simplify only after tests pass.
- [Risk] Helper extraction could introduce naming collisions with imported resolver functions such as `resolve_revision` -> Mitigation: use distinct helper names that communicate Dml-bound context explicitly.
- [Risk] Structural contract tests may lag behind the new boundary and fail despite behavior remaining correct -> Mitigation: update tests in the same change to assert the new no-private-helper rule directly.
- [Trade-off] Module-level helper functions are less encapsulated than instance-private methods -> Mitigation: keep them file-local and narrowly scoped to `dml.py`.

## Migration Plan

1. Inventory all `Dml._...` helper methods and group them by role: ops dispatch, payload building, revision resolution, and remote setup.
2. Introduce equivalent module-level helper functions in `dml.py` with explicit `dml` parameters where state/context is required.
3. Rewrite namespace methods and `Dml` public methods to call the new helper functions.
4. Remove the replaced private helper methods from `Dml` and the private helper method from `_DagNamespace`.
5. Update structural tests to validate the new boundary while preserving existing behavioral assertions.
6. Run targeted tests for `dml`, CLI-facing `Dml` workflows, and contract suites that inspect the shared boundary.

Rollback strategy:

- Revert the helper extraction as one change if behavior or lifecycle regressions appear, then re-apply with tighter test coverage around the affected helper family.

## Open Questions

- Should the implementation keep `_OpsProxy` as a file-local helper or replace it with direct dispatch helpers everywhere?
- Should tests assert an explicit allowlist of remaining private attrs/methods on `Dml` and namespaces, or only assert the absence of the removed helper names?
