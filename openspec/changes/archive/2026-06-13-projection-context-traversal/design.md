## Context

`daggerml.api` currently offers two different traversal experiences. Open DAGs can stage builtin `get` operations and return real `Node` wrappers, while committed DAGs can only materialize full values and inspect limited node metadata. The old `Node.backtrack(...)` idea targeted provenance recovery through collection construction and builtin selection, but it was clunky and is currently disabled. Separately, `Node.load()` only describes one narrow case: loading the DAG behind an import or function node.

The desired public model is provenance-oriented rather than builtin-oriented. Users should be able to inspect a committed DAG result, project into nested dict/list structure, and ask for the nearest or rooted non-builtin function/import DAG context that produced that value. The implementation should stay in `api.py` and reuse existing `dml.dag.describe(...)`, `dml.dag.describe_node(...)`, and `dml.dag.get_node(...)` read APIs rather than changing storage or runtime protocols.

## Goals / Non-Goals

**Goals:**
- Replace `Node.load()` with `Node.context(root: bool = True)` as the public provenance-entrypoint.
- Treat builtin collection construction and builtin selection DAGs as transparent implementation details when resolving context.
- Add a read-only `Projection` object for committed-DAG subvalue traversal when no persisted node ref exists for the projected path.
- Keep `Projection` node-like only for ex-post interrogation: `.value()`, `.context()`, nested indexing, `type`, `keys()`, iteration, and length.
- Keep the change inside `src/daggerml/api.py` and the public docs/tests unless implementation work proves a missing read API.

**Non-Goals:**
- Expose builtin DAG contexts as part of the main public traversal surface.
- Make `Projection` participate in mutation, `Dag.call`, codecs, function invocation, or ref-based identity semantics.
- Change persisted DAG/node schemas, runtime staging behavior, or remote protocols.
- Reintroduce `backtrack(...)` as a separate public method.

## Decisions

### Use one provenance verb: `context(root=...)`

`context()` becomes the single public provenance verb.

- `context(root=False)` returns the nearest DAG reached after backtracking through builtin-produced structure and stopping at the first non-builtin function/import provenance boundary.
- `context(root=True)` repeats that process recursively until provenance no longer crosses a non-builtin function/import boundary.

This is clearer than keeping separate `source()` and `load()` verbs because both operations are about finding the DAG context behind a value rather than returning intermediary builtin nodes.

Alternative considered: keep `source()` plus rename `load()` to `context()`. Rejected because it makes users learn two provenance verbs whose outputs are tightly coupled.

### Introduce `Projection` as a sibling of `Node`, not a fake node

Projected committed-DAG reads may point at subvalues that never existed as standalone persisted nodes. `Projection` should therefore be a sibling read-only wrapper rather than a `Node` subclass with synthetic ref identity.

`Projection` holds:
- the owning `Dag`
- a real base `Node`
- a projected path of string/int traversal steps
- optional cached shape metadata derived from the projected value

Alternative considered: fake refs or a `Node` subclass without true node identity. Rejected because ref-bearing APIs, equality, codecs, and mutation helpers would become misleading.

### Keep open-DAG and committed-DAG indexing under one surface

The public `[]` surface should remain natural:

- open DAG collection access may continue to return real `Node` results backed by staged builtin `get` nodes
- committed DAG collection access should return `Projection` when no direct persisted node exists for the selected path

This keeps read syntax consistent while allowing different implementations under the hood.

Alternative considered: require a separate projection method for committed DAGs. Rejected because it would make normal inspection awkward and expose storage details to callers.

### Resolve projection values and context purely through read-side traversal

`Projection.value()` should resolve by materializing the base node value and applying the stored path in Python. `Projection.context(...)` should resolve by replaying provenance traversal from the base node and path without staging new builtin nodes.

The provenance algorithm should:
- treat builtin collection construction and builtin selection as transparent
- map projected dict/list paths back to original argument nodes when possible
- stop at the nearest non-builtin import/fn DAG for `root=False`
- recurse through nested import/fn chains for `root=True`

Alternative considered: stage temporary builtin reads even for committed DAGs. Rejected because committed DAG interrogation should remain read-only and API-local.

## Risks / Trade-offs

- [Projection behavior diverges from real `Node` behavior] -> Keep the supported `Projection` surface explicit and refuse mutation/call/ref-like operations.
- [Provenance recovery through builtin-produced structures may be incomplete for some builtin shapes] -> Scope the first implementation to collection construction and builtin `get`/selection flows discussed in the change, and document unsupported provenance shapes clearly.
- [Rooted context traversal may be expensive for large nested values] -> Keep traversal path-based, reuse existing node descriptions, and only materialize values when needed for projection reads.
- [Renaming `load()` may break callers] -> Update docs and tests together, call out the rename in the proposal/specs, and decide during implementation whether a temporary compatibility alias is warranted.

## Migration Plan

1. Introduce `Node.context(root=...)` and `Projection` in `api.py`.
2. Update committed collection-read code paths to return `Projection` where appropriate.
3. Update API docs and examples to use `context()` instead of `load()` for provenance traversal.
4. Update or replace API contract tests to cover both real `Node` and `Projection` interrogation.
5. Decide during implementation whether `load()` is removed immediately or retained briefly as a compatibility shim; prefer removal unless maintained callers require a bridge.

## Open Questions

- Whether `load()` should be removed immediately or kept as a short-lived compatibility alias is still an implementation-time decision.
