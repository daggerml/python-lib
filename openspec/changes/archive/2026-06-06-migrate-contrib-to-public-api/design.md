## Context

`daggerml.contrib` is the extension layer for delayed authoring, adapters, executors, S3 helpers, status, and tests. It currently mixes public API usage with private `_core` imports and older runtime-protocol assumptions.

The current public API already supports the worker DAG construction needed by contrib execution workers:

```
api.temporary(remote_root=...)
  -> temp Dml instance
api.new(dml=temp_dml, cache_key=..., execution_id=...)
  -> execution-aware worker DAG with argv materialized by runtime.create(...)
```

That means this migration does not require new public API or core runtime functionality. The work is to make contrib conform to the existing public/session APIs and the existing runtime adapter envelope.

The implementation boundary is strict: implementation changes for this change are allowed only under `src/daggerml/contrib/**` and contrib-scoped tests/docs if test or documentation updates are necessary. No modifications are allowed to `src/daggerml/api.py`, `src/daggerml/_core/dml.py`, `src/daggerml/_core/**`, package-root exports, or unrelated code.

## Goals / Non-Goals

**Goals:**

- Migrate contrib DAG/session operations to existing public APIs wherever those APIs already cover the operation.
- Remove contrib worker DAG creation based on `argv_ptr`.
- Have contrib adapters/executors accept the existing core adapter envelope without requiring changes to core envelope production.
- Keep `temporary()` as repository/session setup only.
- Preserve existing contrib user-facing behavior where possible.
- Keep the implementation entirely contrib-scoped.

**Non-Goals:**

- Do not add parameters or behavior to `daggerml.api.new()` or `daggerml.api.temporary()`.
- Do not modify `Dml`, `Dml.runtime`, `IndexOps`, `ExecutionState`, or any `_core` module.
- Do not change adapter envelope production in core runtime code.
- Do not update package-root exports solely to support this migration.
- Do not broaden the migration into unrelated cleanup outside contrib.

## Decisions

### Decision: Contrib adapts to the existing runtime envelope

Contrib adapter parsing should accept the envelope emitted by the current runtime implementation. The migration should not require core runtime changes to reintroduce `argv_ptr` or reshape adapter payloads.

Alternative considered: change core to emit the older contrib envelope, including `argv_ptr`. That would make some old contrib code easier to preserve, but it would violate the contrib-only implementation boundary and keep stale protocol surface alive.

### Decision: Worker DAG construction uses `cache_key` and `execution_id`

Contrib workers that need `dag.argv` should create a temporary `Dml` first, then call `api.new(dml=temp_dml, cache_key=cache_key, execution_id=execution_id)`. `temporary()` should not accept `execution_id`, and `new()` should not accept `argv_ptr`.

Alternative considered: add `argv_ptr` support to `api.new()` or execution context support to `temporary()`. That is unnecessary because the current runtime already materializes active argv by `cache_key` when creating an execution-aware index.

### Decision: Keep protocol compatibility code inside contrib

If contrib needs decoding helpers for runtime envelopes, runnable dictionaries, or adapter results, those helpers should live under `src/daggerml/contrib/**`. They must not be added to core or public API files as part of this change.

Alternative considered: move adapter-envelope types into public API. That would require a separate design because adapter wire contracts are runtime protocol concerns rather than general Python authoring APIs.

### Decision: Treat forbidden-file changes as blockers

If implementation discovers that migration cannot be completed without modifying non-contrib files, implementation must stop. The correct next step is a new or amended proposal that explicitly changes the implementation boundary, not an opportunistic edit outside contrib.

## Risks / Trade-offs

- Runtime/source/spec drift may already exist around adapter envelope fields such as `argv_ptr`, `status`, and `lifecycle` -> Mitigation: this change documents contrib as adapting to the existing source behavior and constrains implementation away from core changes.
- Contrib-only compatibility helpers may duplicate some runtime knowledge -> Mitigation: keep helpers narrow, local to adapter parsing/result normalization, and covered by contrib tests.
- Some existing tests may assert old `argv_ptr` payloads -> Mitigation: update only contrib-scoped tests to assert `cache_key` plus `execution_id` behavior.
- A real core bug may be discovered during migration -> Mitigation: stop and propose a separate non-contrib change rather than violating this change's boundary.

## Migration Plan

1. Update contrib imports and helper usage to prefer `daggerml.api` or public package-root exports when existing APIs are sufficient.
2. Update contrib adapter payload parsing to accept the current runtime envelope without `argv_ptr`.
3. Update script and CloudFormation worker DAG creation to use `api.temporary(...)` followed by `api.new(..., cache_key=..., execution_id=...)`.
4. Update nested executor forwarding to preserve the current runtime envelope fields expected by nested adapters.
5. Run contrib-focused tests and any existing runtime smoke tests that exercise contrib execution, without modifying non-contrib source.

Rollback is straightforward because the migration is contrib-contained: revert the contrib-only implementation change. If a non-contrib change is required, this OpenSpec change should be paused rather than partially expanded.

## Open Questions

- Should contrib adapter parsing support old `argv_ptr` payloads as a temporary backward-compatibility path, or should the migration fail fast on stale payloads?
- Which existing tests are considered contrib-scoped if they live outside a contrib-named test directory but only exercise contrib behavior?
