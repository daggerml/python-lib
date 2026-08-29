## Context

`Tree` currently stores only `dags: dict[str, Ref]`, while commit and runtime operations assume that map is the complete tree state. `Dml.show()` and `Dml.log()` expose only DAG mappings. See proposal.md for motivation and `specs/dag-tree-tags/spec.md` for the behavior contract.

## Goals / Non-Goals

**Goals:**

- Store opaque, snapshot-specific tags for named DAG entries with minimal change to the persistent model.
- Preserve tags correctly through every operation that constructs a successor tree.
- Provide the smallest public mutation surface for users to organize existing DAG entries.
- Deliberately reject pre-tag tree payloads rather than supporting a storage migration.

**Non-Goals:**

- Defining tag vocabularies, research schemas, node requirements, or tag-driven runtime behavior.
- Adding tag search, filtering, indexing, or a dedicated CLI implementation.
- Supporting historical databases or old remote tree objects.
- Automatically merging independent tag changes.

## Decisions

### Use a required parallel `Tree.tags` map

`Tree` will contain `dags: dict[str, Ref]` and required `tags: dict[str, list[str]]`. The validation invariant is `set(tags) <= set(dags)`, with string keys and tag values. No default value is supplied, so missing persisted fields fail normal object construction.

Keeping DAG refs in `dags` avoids changing every consumer from a ref to a wrapper object. A `DagEntry(ref, tags)` value would co-locate data but would broaden the public and internal type change substantially. Tags on `Dag` itself were rejected because classification belongs to tree membership and may differ across commits.

### Treat tags as opaque ordered labels

The storage model retains user-provided list order and does not reserve names such as `research.v0`. `add_tag` avoids adding a duplicate label and `remove_tag` removes the requested label; direct model validation does not impose a schema beyond the required structural types and key-subset invariant.

This preserves the option for external validators or future conventions without making the core repository responsible for their meaning.

### Make tag mutations branch commits

`Dml.dag.add_tag(dag: str, tag: str) -> Ref` and `Dml.dag.remove_tag(dag: str, tag: str) -> Ref` update the current attached branch under the existing HEAD lock. A changing operation writes a successor `Commit` with the current commit as parent and returns its ref. An idempotent operation returns the current commit unchanged.

This reuses the repository's branch ownership, history, author, and locking rules. A mutable side store would lose commit lineage and require a new lifecycle. Tags are metadata on a named tree entry, so the methods identify the entry by DAG name rather than by immutable DAG ref.

The generated CLI reflects public `Dml.dag` methods, so these APIs are also
available as generated `dml dag add-tag` and `dml dag remove-tag` commands. No
tag-specific CLI routing or query commands are added.

### Propagate tags as part of an internal tree entry

Private commit diff/patch logic will compare and apply each name's `(dag_ref, tags)` pair so rebase, merge, and revert do not discard tag-only edits. A differing tag list for the same name is a conflict, even when the DAG ref is unchanged. The public DAG-only diff payload remains unchanged because tag-specific diff output is out of scope.

Checkout or replacement removes tags at the destination name, preventing metadata for the old DAG from being applied to a different DAG. DAG deletion removes the corresponding map entry. New trees, including initial and runtime-base trees, are constructed with `tags={}`.

### Expose raw tags through existing history payloads

`CommitDescription` and the payload returned by `show` and `log` gain `tags: dict[str, list[str]]`. This is inspection, not a query feature: callers receive stored labels and determine their own meaning.

## Risks / Trade-offs

- [Tag-only commits are absent from the current public DAG diff] -> Preserve them internally for history transformations and expose their state through `show` and `log`; defer tag diff API design.
- [Concurrent metadata edits conflict instead of unioning] -> Retain existing name-level tree conflict semantics and avoid inventing removal/order merge rules.
- [No compatibility path makes old repositories unreadable] -> This is an explicit v0 decision; release notes and docs must state the storage break.
- [Tags can be semantically invalid] -> Keep enforcement external to the core; future schema validators can build on the stored labels.

## Migration Plan

No migration or compatibility path will be provided. New code writes `tags={}` for every new tree. Existing persisted tree payloads without `tags` intentionally fail to decode. Rollback requires restoring code from before the change and using repositories created by that code; repositories written with the new required field are not a compatibility target.
