## Context

Committed dict and list indexing returns a `Projection` that stores a real committed base `Node` plus an ordered path of string keys, integer indices, or normalized slice bounds. This representation supports read-only value and provenance inspection, but `apply_codecs()` currently has no built-in codec for it. As a result, a value selected from a function context cannot be fed back into the active caller DAG even though the projection contains everything needed to reconstruct the selection.

The public codec pipeline already receives the active `Dag`, recursively normalizes values regardless of which staging entrypoint encountered them, imports committed `Node` values through `NodeCodec`, and stages builtin `daggerml:get` calls through `Dag._call_builtin`. The change should compose those existing mechanisms rather than teach `Dag.put`, `Dag.call`, or collection normalization about projections individually.

## Goals / Non-Goals

**Goals:**

- Make projections originating from the active target DAG's `Dml` instance codec-encodable.
- Insert the projection's committed base as an `ImportNode` in the target DAG.
- Replay the projection path in order as builtin `get` access nodes and return the final node ref.
- Preserve recursive codec behavior so direct values, nested collections, runnable fields, and call arguments require no projection-specific entrypoint logic.
- Document the materialization behavior and resulting graph shape.

**Non-Goals:**

- Give `Projection` an independent persisted ref or make it a `Node` subclass.
- Mutate a committed source DAG while indexing or encoding a projection.
- Add special projection branches to `Dag.put`, `Dag.call`, or runtime storage operations.
- Define compatibility for projections originating from another `Dml` instance or repository.
- Deduplicate independently requested imports or access chains beyond existing content-addressed storage and runtime behavior.

## Decisions

### Implement projection reuse as a built-in codec

`ProjectionCodec.can_encode()` will recognize `Projection` values, and the codec will be included in the built-in registrations returned by `codecs()`. Consequently, every path already governed by `apply_codecs()` receives projection support automatically.

Alternative considered: handle projections directly in `Dag.put` and `Dag.call`. Rejected because it would duplicate dispatch rules, miss nested values, and contradict the purpose of the codec normalization layer.

### Import the base and replay the complete path

Encoding will first use the existing node encoding behavior for `projection.base`. For a normal committed projection this inserts an unnamed `ImportNode` for the base node into the active target DAG. The codec will then iterate over `projection.path`, call the builtin `daggerml:get` operation with the previous result and the current path step, and return the last resulting ref.

For a path `("my_key", "my_key1")`, the target graph is:

```text
ImportNode(base)
      |
      v
get(base, "my_key")
      |
      v
get(previous, "my_key1")
```

Replaying the complete stored path is preferred over materializing `projection.value()` because materialization would discard source-node provenance and store a copied literal. It is also preferred over adding provenance-based path compaction because the projection's base and path are already the exact public selection recipe.

### Return the final ref to normal literal staging

The codec returns the final imported or access-node `Ref`. The enclosing normalization and runtime literal insertion paths already accept refs that belong to the active DAG, so direct `Dag.put(..., name=...)` can bind the requested name without creating a second value node. Nested normalization can embed the same ref using existing collection construction behavior.

### Retain ordinary codec error semantics

The codec relies on the existing `NodeCodec`, import operation, builtin staging, and `apply_codec()` error boundary. Repository-domain failures continue to propagate according to the codec contract, while other codec failures are reported as `CodecError`. No separate projection error hierarchy is introduced.

### Keep source traversal read-only

Creating and extending a `Projection` continues to perform only committed-DAG reads. Mutation begins only when the caller supplies that projection to an operation that already performs codec normalization, and all generated nodes belong to the active target DAG.

## Risks / Trade-offs

- [A long projection path inserts multiple builtin function nodes] -> Preserve one access node per path step because this exactly records the user's selection and retains inspectable provenance.
- [Encoding the same projection repeatedly can request repeated import/access chains] -> Rely on existing content-addressed object identity and runtime behavior rather than adding stateful codec caching.
- [A projection from a different `Dml` instance cannot be imported by local refs] -> Limit the contract to projections from the target DAG's `Dml` instance and leave existing repository/import failures intact outside that contract.
- [Codec tests replace the process codec registry with a fixture] -> Register `ProjectionCodec` in the isolated test registry so contract tests exercise production normalization order.

## Migration Plan

1. Complete and register `ProjectionCodec` in the existing public codec implementation.
2. Add contract and live-runtime coverage for import-plus-access reconstruction and recursive normalization.
3. Update projection and codec documentation to describe supported reuse.
4. No data migration or rollout compatibility layer is required because persisted object shapes do not change.

## Open Questions

None. Codec dispatch, supported repository scope, and import-plus-access graph semantics are defined by this change.
