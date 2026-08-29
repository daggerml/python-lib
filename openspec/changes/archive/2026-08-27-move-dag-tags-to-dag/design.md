## Context

See proposal.md for motivation. Tags are currently a required `Tree` field keyed by a DAG's name in a commit. Execution state publishes only a `dag:` result ref, so that representation cannot attach tags to executor-created results. The public wrapper currently holds tags separately and applies them after committing through branch-local tree mutations.

## Goals / Non-Goals

**Goals:**

- Make tags durable content of every DAG, including successful and error function-result DAGs.
- Keep the normalized tag set observable through the same `api.Dag.tags` surface for live and loaded DAGs.
- Permit tag updates only while an index remains active.
- Carry script-funk tag declarations to worker DAG creation before any user source executes.

**Non-Goals:**

- Preserve repository compatibility with the current tree-tag object shape.
- Support retagging a completed DAG or branch-local tag views for one DAG name.
- Define tag query, schema, hierarchy, or interpretation semantics.
- Make decorator tags cache-key-neutral; tags remain part of the resolved runnable unless cache identity is changed in a later design.

## Decisions

### Persist tags in `Dag`, not `Tree`

Add a required `tags: list[str]` field to the core DAG object and remove `Tree.tags`. Core validation and a shared normalization boundary produce sorted, duplicate-free strings. A committed DAG ref then carries its own tags through execution state, remote graph transfer, imports, and any tree that names it.

Keeping both fields would create competing meanings for "DAG tags." Copying tree tags into execution state would still fail to preserve tags whenever a result is used without a named tree entry. A separate mutable tag sidecar would conflict with content-addressed DAG identity and remote graph closure.

### Initialize and mutate tags through runtime indexes

Extend runtime creation with optional tags and initialize newly created core DAGs with their normalized values. Add runtime add-tag and remove-tag methods that mutate only an active index's current DAG and reuse the index mutation lifecycle guard. They normalize idempotently and reject frozen or completed targets.

The public `Dag.tags` becomes a core-backed view rather than wrapper-only commit metadata: it resolves the current partial DAG for a live index and the committed DAG ref for a loaded wrapper. Public `new` passes tags into runtime creation. `resume` accepts no tags because its frozen DAG already stores them.

Keeping the API wrapper's existing independent mutable field would allow it to diverge from runtime mutations and lose tags through freeze/resume. Supporting mutation of completed DAGs would require cloning a content-addressed DAG and repointing names, which is explicitly out of scope.

### Commit tags atomically with the DAG

Index commit persists the current core DAG, including tags, before publishing execution state or creating a named tree entry. It no longer needs a post-commit tag mutation pass. Error-result commits follow the same path.

This replaces the current non-atomic sequence of commit followed by `Dml.dag.add_tag`. It also removes tag-only commits and the merge, rebase, checkout, revert, deletion, and inspection logic that operates on a parallel tree-tag mapping.

### Propagate script-funk tags to worker creation

`funkify` accepts script tags as script-executor declaration metadata. The script resolver retains normalized tags in its concrete runnable. The script executor passes those tags through the worker launch payload or command line, allowing `run_payload` to call public DAG creation with tags alongside the cache key and execution ID.

Passing tags only through `dag.argv` is insufficient: the worker creates that DAG before it can inspect `argv`. Assigning them after DAG creation would not meet the initialization/freeze invariant and risks early result paths observing an untagged DAG.

### Remove tree-tag public and inspection surfaces

Remove `Dml.dag.add_tag` and `remove_tag`, the tree tag field, and commit-description `tags`. `show` and log report only their commit-owned data. Consumers inspect a returned DAG's description or public wrapper to read tags.

Retaining the old methods with copy-and-repoint semantics was rejected because a name-targeted operation would imply that tags remain associated with a tree entry and would split equivalent DAG references unexpectedly.

## Risks / Trade-offs

- [Persisted object incompatibility] Existing trees lack the new shape and existing DAGs lack tags. → Treat this as the deliberate v0 format break documented in the proposal; update persistence contract tests and release notes.
- [Public wrapper tag mutation ambiguity] A mutable Python list cannot safely mutate durable active state. → Expose tags as a read snapshot and use runtime add/remove operations as the mutation contract.
- [Changed cache identity] Resolved runnable tags participate in the argv datum used for cache identity. → Document and test the behavior; defer a non-cache metadata channel to a separate change if reuse across tag changes is required.
- [Nested executor propagation] Script workers can run beneath Docker, SSH, or Batch. → Carry tags on the normal script-worker payload path and add nested execution coverage.
- [Generated CLI surface changes] Runtime and DAG namespace changes alter generated help and schemas. → Regenerate and verify CLI contracts.

## Migration Plan

1. Release the storage and API break together; no in-place conversion or fallback decoding is provided.
2. Remove tree-tag APIs and documentation, then expose runtime tag operations and DAG-description tags.
3. Update public authoring callers to set tags at DAG or funk creation, and remove the `tags` argument from resume calls.
4. Update tests, examples, generated CLI contracts, and human-facing documentation.
5. Rollback requires restoring the prior release or repository state; mixed old/new persisted objects are unsupported.
