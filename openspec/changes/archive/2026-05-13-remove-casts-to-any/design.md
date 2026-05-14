## Context

The current `cast(..., Any)` sites cluster in two places. In `daggerml.contrib.api`, they wrap dynamic class metadata writes, `__init__` wrapping, and dagclass member staging/calling. In the execution runtime, they erase the concrete execution-status type when building or merging `ExecutionRecord` dictionaries. Tests mirror the same pattern by forcing values through `Any` even when the cast changes nothing.

This change should stay as small as possible: remove the no-op `cast(..., Any)` calls, keep the surrounding behavior intact, and only make additional local edits if removing a cast exposes a real issue.

## Goals / Non-Goals

**Goals:**
- Remove every current `cast(..., Any)` occurrence from source and tests.
- Preserve current runtime behavior for `api.dagclass`, `api.run`, `api.funkify`, and execution-record persistence.
- Keep the implementation small and local to the affected modules.

**Non-Goals:**
- Redesign `api.dagclass`, `api.run`, or adapter execution semantics.
- Broaden the change into a full repo-wide typing cleanup beyond the current `cast(..., Any)` sites.
- Introduce helper abstractions, compatibility layers, or alternate code paths just to compensate for removing `cast(..., Any)`.

## Decisions

### Remove the `Any` casts directly
The implementation should delete each `cast(..., Any)` call and keep the surrounding expression as-is whenever that remains valid. The cast is a type-checking no-op at runtime, so the default approach is simple removal rather than replacement.

Alternative considered: replace removed casts with helper typing layers. Rejected because that adds scope without serving the stated goal.

### Only make local follow-up edits when deletion alone is insufficient
If deleting a cast causes a concrete type-check or test failure, the implementation should fix that exact line in the smallest possible way. The change should not expand into broader typing refactors.

Alternative considered: widen signatures or add new helper APIs. Rejected because it spreads or grows the change unnecessarily.

### Keep test cleanup equally direct
Tests should stop using `cast(..., Any)` and instead pass the concrete value directly unless a specific test requires a different minimal local adjustment.

Alternative considered: leave test-only `Any` casts in place. Rejected because the cleanup should apply everywhere.

## Risks / Trade-offs

- [Some sites may not type-check after raw cast removal] -> Make the smallest possible local fix only where needed.
- [Dynamic class mutation is awkward under static typing] -> Do not preemptively abstract it; only touch the exact lines that break.
- [No runtime behavior change means regressions could be subtle] -> Verify with the focused contrib and execution-state test coverage that already exercises these paths.
