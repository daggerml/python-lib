## Why

Tags currently annotate a DAG's name in a commit tree rather than the DAG object itself. Executor-produced result DAGs are published only as `dag:` refs, so tree-entry tags cannot survive result publication, cache reuse, imports, or a DAG being named elsewhere.

## What Changes

- **BREAKING** Move opaque tags from `Tree.tags` to a required, normalized `Dag.tags` list. Tags become immutable DAG metadata rather than labels on a named commit-tree entry.
- **BREAKING** Remove tree-tag data from commit inspection and history output, and remove `Dml.dag.add_tag()` and `Dml.dag.remove_tag()`.
- Add `Dml.runtime.add_tag()` and `Dml.runtime.remove_tag()` for mutating tags on active, non-frozen indexes before they are committed.
- Make `Dml.new(..., tags=...)` initialize the active DAG's tags. Loaded and live public `Dag` wrappers expose the same hydrated `tags` list.
- **BREAKING** Remove `tags` from `Dml.resume()`; freeze and unfreeze preserve the active DAG's tags.
- Allow script funks to declare DAG tags during funkification and carry those tags through script-worker creation so every published function result DAG retains them.
- Require tag lists to contain unique strings in lexicographic order.

## Capabilities

### New Capabilities
- `funk-dag-tags`: Script funks can declare normalized result-DAG tags that are propagated into every executed result DAG.

### Modified Capabilities
- `dag-tree-tags`: Replace named tree-entry tag storage and mutation with intrinsic DAG tag storage and active-runtime mutation.
- `runtime-index-freezing`: Preserve intrinsic DAG tags across freeze and unfreeze, and remove resume-time tag input.

## Impact

- Affected core persistence and history code: `daggerml._core.types`, `dag`, `index`, `commit`, and `dml`.
- Affected public authoring APIs: `daggerml.api.new`, `load`, `resume`, and `Dag.tags`.
- Affected contrib API and script executor: `daggerml.contrib.api.funkify` and `daggerml.contrib.executors.script`.
- Existing repositories and serialized objects are incompatible with the changed required `Dag` and `Tree` shapes.
- Documentation, CLI-generated API surfaces, contract tests, and tag-related tests require updates.
