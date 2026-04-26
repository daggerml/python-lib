## Context

The contrib executor subsystem (`SshExecutor`, `DockerExecutor`, etc.) validates kwargs before building execution commands. Some kwargs may be provided as "live" values (`Node` — a resolved DAG node) or as "deferred" values (`DelayedRef`, `DelayedLoad`, `DelayedRunnable` — values that are only resolved at DAG execution time). Currently `SshExecutor._validate_kw` checks for deferred values using `isinstance(x, DelayedActionCodec)`, which is the codec wrapper, not the actual user-facing `Delayed*` types. There is no shared predicate, so each executor must replicate the pattern, risking drift.

All `Delayed*` types and `Node` (public) are defined in `src/daggerml/contrib/api.py` and `src/daggerml/api.py` respectively.

## Goals / Non-Goals

**Goals:**
- Add a single `is_node_like(x)` predicate in `src/daggerml/contrib/api.py` that returns `True` for `Node | DelayedRef | DelayedLoad | DelayedRunnable`
- Update `SshExecutor._validate_kw` to use `is_node_like` for its per-field checks
- Export `is_node_like` so other modules can import it

**Non-Goals:**
- Refactoring all other executor validators (DockerExecutor, ScriptExecutor, BatchExecutor) in this change
- Changing the behavior of `DelayedActionCodec` or any codec logic
- Adding `is_node_like` to the internal `_internal/types.py` layer

## Decisions

**Where to define `is_node_like`**

Place in `src/daggerml/contrib/api.py`, alongside the `Delayed*` type definitions.

Alternatives considered:
- `src/daggerml/api.py` — only knows about `Node`, not `Delayed*`; would require importing contrib types into core API (wrong direction)
- `src/daggerml/_internal/types.py` — lowest-level home, but `Delayed*` types are in contrib and should not leak into internal
- Separate utils module — unnecessary indirection for a one-liner predicate

**Predicate signature**

```python
def is_node_like(x: object) -> bool:
    return isinstance(x, (Node, DelayedRef, DelayedLoad, DelayedRunnable))
```

Simple, no ABC or protocol needed at this stage.

**SshExecutor import**

`SshExecutor` already imports from `daggerml.contrib.api`; add `is_node_like` to that import.

## Risks / Trade-offs

- [Risk] Future `Delayed*` types added without updating `is_node_like` → Mitigation: keep the predicate next to the type definitions so it is easy to spot during code review
- [Trade-off] Not updating other executors now keeps the change small and reviewable; they remain inconsistent for now

## Migration Plan

No data migration needed. Change is purely additive (new function) plus a one-line update in `_validate_kw`. No deprecation or rollback concern.

## Open Questions

- Should `is_node_like` also cover `MockNode` from `contrib/testing.py`? Current answer: no — `MockNode` is test infrastructure, not a production node-like type. Can be revisited.
