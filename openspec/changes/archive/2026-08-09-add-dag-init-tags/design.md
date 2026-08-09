## Context

The public `Dag` wrapper is a dataclass that carries commit metadata and delegates finalization to `Dml.runtime.commit()`. Tree-entry tags already exist and are mutated through `Dml.dag.add_tag(dag, tag)`, which creates a successor commit only when a tag is absent. See `proposal.md` for motivation and `specs/dag-tree-tags/spec.md` for the behavior contract.

## Goals / Non-Goals

**Goals:**

- Keep the new state on the public wrapper with a `None` default so existing construction remains unchanged.
- Reuse the established tag mutation API after DAG publication.
- Make commit and failure ordering explicit and testable.

**Non-Goals:**

- Making tags part of the mutable runtime index or its atomic commit transaction.
- Changing tag validation, deduplication, storage, or low-level mutation semantics.
- Adding tag arguments to unrelated loading or core storage APIs.

## Decisions

### Store optional tags on `Dag`

Add a `list[str] | None` dataclass field defaulting to `None`. A list matches the persisted ordering contract and distinguishes omission from an explicitly empty list without introducing a new collection type. The implementation only iterates the field, so both forms produce no mutations.

Alternative: normalize to an empty list with a default factory. This is safe but does not preserve the requested `None` default and adds no behavioral benefit.

### Apply tags after runtime commit

`Dag.commit()` first completes its existing runtime commit and updates wrapper state, then invokes `self.dml.dag.add_tag(self.name, tag)` for each supplied tag in order. This guarantees tags target an existing tree entry and directly follows the requested use of the public mutation operation.

Alternative: write tags as part of runtime commit. That could provide atomic publication but would expand core interfaces and duplicate tag mutation rules for a small public-API convenience.

### Preserve normal exception behavior

An underlying commit failure prevents all tag calls. If a later `add_tag()` call fails, its exception propagates; the DAG commit and any earlier successful tag commits remain published. The wrapper's token is still cleared and ref retained because the DAG itself completed successfully.

Alternative: compensate by deleting tags or the committed DAG. Existing history operations are append-only and no atomic multi-operation API exists, so compensation would create more history and could itself fail.

## Risks / Trade-offs

- [Multiple tags create multiple successor commits] -> Reuse the existing API as requested and document the ordering; batching is outside this change.
- [A tag failure can leave partial tag assignment] -> Propagate the error and retain accurate committed wrapper state so callers can inspect or retry with existing tag operations.
- [A mutable input list can be changed before commit] -> Treat the field like existing mutable authoring state and consume its value at commit time; avoid an unnecessary defensive copy.

## Migration Plan

No data migration is required. Add the optional field, tests, and public authoring documentation in one release; rollback removes the convenience field and post-commit calls without changing persisted data created through the existing tag API.
