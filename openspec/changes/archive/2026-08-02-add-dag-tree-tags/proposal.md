## Why

Researchers need a lightweight way to organize and classify committed DAGs without introducing a separate experiment object or imposing a repository-defined research schema. Tree-entry tags preserve the classification and its commit lineage while leaving tag meaning under user control.

## What Changes

- Add required per-DAG-name tags to `Tree`, stored alongside its named DAG references.
- Enforce that each tagged name exists in the tree's DAG map; tag values are otherwise opaque lists of strings.
- Preserve tags through tree and commit operations, including checkout, deletion, merge, revert, and rebase.
- Expose stored tags in commit inspection and history payloads.
- Add `Dml.dag.add_tag` and `Dml.dag.remove_tag` to update tags for named DAG entries on the current attached branch.
- **BREAKING** Require `tags` in all persisted `Tree` payloads. Existing tree objects without this field are unsupported and must fail to decode.
- Do not add tag schemas, tag query/filter APIs, automatic behavior, or compatibility shims.

## Capabilities

### New Capabilities
- `dag-tree-tags`: Store, inspect, and mutate opaque tags attached to named DAG entries in commit trees.

### Modified Capabilities
- None.

## Impact

- Affects the core persistent `Tree` model, tree mutation and history operations, `Dml.show`/`Dml.log` payloads, and the public `Dml.dag` namespace.
- Requires core contract coverage and documentation updates for tree storage and history.
- Does not add dependencies or alter remote object traversal; tags are ordinary tree payload data.
