## Context

`daggerml._internal.dml` currently orchestrates lower-level ops through two internal abstractions: the `DmlOps` facade in `daggerml._internal.ops` and the `_OpsProxy` string-dispatch layer in `daggerml._internal.dml`. The surviving `Dml` surface is already the caller-facing orchestration boundary, so these extra layers mostly wrap DB open/close, concrete ops construction, and remote configuration expansion.

The change must preserve the existing `Dml` and namespace API exactly. The simplification is intentionally internal-only: remove indirection, reduce code size, and make module-level helper functions in `daggerml._internal.dml` construct the owning ops classes directly.

## Goals / Non-Goals

**Goals:**

- Remove `DmlOps` as an internal repository/session facade.
- Remove `_OpsProxy`, string-based factory dispatch, and helper layers that exist only to reach concrete ops classes.
- Keep `Dml` as the sole caller-facing orchestration boundary without adding any new public methods, properties, or namespaces.
- Preserve explicit `remote.root` threading for remote-aware helpers and ops classes.
- Update specs, tests, and docs so they describe the simplified construction path rather than the removed facade.

**Non-Goals:**

- No new `Dml` APIs or namespace reshaping.
- No compatibility shims, alias exports, or transitional wrappers for `DmlOps`.
- No change to commit, DAG, runtime, cache, GC, or remote business semantics beyond how their ops instances are constructed.
- No redesign of the lower-level ops class public methods.

## Decisions

### Decision: `Dml` helper functions will construct concrete ops classes directly

`daggerml._internal.dml` will own the DB lifecycle helpers and the module-level functions that instantiate `HeadOps`, `CommitOps`, `DagOps`, `NodeOps`, `IndexOps`, `CacheOps`, `GcOps`, and `RemoteOps` directly.

Rationale:

- This removes both the facade layer and the string-dispatch layer.
- The resulting code matches the actual subsystem ownership documented in the repo: `Dml` orchestrates, concrete ops implement behavior.
- It keeps internal construction readable at the call site instead of hiding it behind factory names.

Alternatives considered:

- Remove `DmlOps` but keep `_OpsProxy`: rejected because it preserves string dispatch and does not meaningfully simplify the orchestration path.
- Replace `DmlOps` with another lightweight facade: rejected because it renames the same abstraction cost instead of deleting it.

### Decision: The `Dml` public and namespaced surface remains frozen

This change will not add methods or properties to `Dml` or any of its namespaces. All simplification happens inside existing module-level helpers and namespace implementations.

Rationale:

- The goal is simplification, not surface expansion.
- Existing callers already have the orchestration boundary they need.
- Preserving the current surface keeps the change narrowly focused on internal construction.

Alternatives considered:

- Add new convenience methods for direct ops access: rejected as counter to the stated scope and unnecessary once helper construction is simplified.

### Decision: Remote-aware construction continues to take explicit resolved `remote.root`

Helpers that instantiate `IndexOps`, `CacheOps`, `RemoteOps`, and other remote-aware components will continue to pass normalized `remote.root` explicitly. The removal of `DmlOps` will not reintroduce implicit config lookups at lower layers.

Rationale:

- It preserves the existing explicit-configuration contract.
- It keeps remote-aware behavior consistent with the existing specs.
- It avoids sliding back into environment-driven construction hidden inside lower-level components.

Alternatives considered:

- Let lower-level ops resolve config themselves: rejected because it weakens the current boundary and increases hidden coupling.

### Decision: Spec and documentation language will stop naming `DmlOps` as an active boundary

Any specs or docs that currently describe `DmlOps` as the surviving internal orchestration boundary will be rewritten to point at the shared `Dml` workflow and direct helper-based ops construction.

Rationale:

- The artifacts should describe the architecture that remains after the simplification.
- Keeping `DmlOps` in the docs after deleting it would preserve conceptual dead code.

## Risks / Trade-offs

- Removing `DmlOps` also removes a single place that bundled DB lifecycle and ops factories. → Keep DB open/create helpers explicit in `daggerml._internal.dml` so construction remains centralized without reintroducing a facade.
- Direct construction can expose duplicated remote parsing or config-expansion logic that the facade previously hid. → Consolidate the construction path around shared helper functions in `daggerml._internal.dml` and update tests around remote-aware helper behavior.
- Existing specs and tests may still import or name `DmlOps`. → Update OpenSpec deltas, docs, and contract tests in the same change so the architecture and verification story stay aligned.

## Migration Plan

1. Update OpenSpec artifacts to define `Dml` as the surviving orchestration boundary and remove `DmlOps` language.
2. Delete `DmlOps` and `_OpsProxy`-style helper layers.
3. Rewrite `daggerml._internal.dml` helper construction around direct concrete ops instantiation and explicit DB lifecycle helpers.
4. Update tests and docs to target the direct-construction model.

Rollback strategy: revert the change before release. No compatibility layer or persistent data migration is planned.

## Open Questions

None.
