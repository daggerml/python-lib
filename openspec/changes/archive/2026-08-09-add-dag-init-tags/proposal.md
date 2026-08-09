## Why

Authors who know a DAG's classification at creation time currently must commit the DAG and then issue separate low-level tag mutations. Allowing tags on the public `Dag` wrapper keeps that metadata adjacent to DAG authoring while reusing the existing tree-tag behavior.

## What Changes

- Add an optional `tags` initialization argument to `daggerml.api.Dag`, defaulting to `None`.
- After a successful named DAG commit, call `dml.dag.add_tag()` once for each provided tag.
- Do not perform tag mutations when tags are omitted or empty.
- Document and test the public authoring behavior, including commit ordering and tag-mutation failures.

## Capabilities

### New Capabilities

None.

### Modified Capabilities

- `dag-tree-tags`: Extend tag assignment so tags supplied while initializing a public `Dag` wrapper are added to its named tree entry after commit.

## Impact

- Public API: `daggerml.api.Dag` gains an optional constructor field for tags.
- Commit flow: `Dag.commit()` performs existing `Dml.dag.add_tag()` mutations after the DAG has been committed.
- Tests and user-facing Python authoring documentation require updates.
- No storage format, core tag mutation API, or dependency changes are required.
