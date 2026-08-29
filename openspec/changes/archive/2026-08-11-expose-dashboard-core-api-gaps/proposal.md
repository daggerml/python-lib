## Why

Dashboard and other inspection clients cannot currently read executor launch state, enumerate branch and tag tips through the required local, endpoint, and dependency source matrix, or inspect the upstream of a non-current branch through the public `Dml` API. These gaps force callers toward private core objects and prevent complete, side-effect-free repository views.

## What Changes

- Add `dml.runtime.read_launch_state(execution_id)` to return the persisted executor resume-state JSON object for one execution.
- Extend `dml.branch.list()` and `dml.tag.list()` with independent keyword-only `remote` and `dep` source selectors and return a list of `{"name": str, "commit": Ref}` items carrying exact commit tips.
- Define remote branch and tag enumeration as bounded and read-only: listing refs does not fetch or materialize objects, update tracking refs, or initialize a remote descriptor.
- Add `dml.branch.get_upstream(name)` to inspect the configured upstream of any local branch; tags gain no upstream operation.
- **BREAKING**: local `branch.list()` and `tag.list()` results change from `list[str]` to `list[RefListItem]`, where each item has exact shape `{"name": str, "commit": Ref}`.

## Capabilities

### New Capabilities

None.

### Modified Capabilities

- `unified-dml-surface`: Add the public runtime launch-state read, source-selectable branch/tag listing, and arbitrary branch upstream lookup methods and result contracts.
- `generated-dml-cli`: Preserve mutual exclusion for revision-source commands while allowing generated branch/tag list commands to accept `--remote` and `--dep` together.
- `runtime-execution-records`: Add direct caller-facing inspection of persisted launch state without reshaping it into lifecycle or graph data.
- `remote-project-refs`: Define local, fetched, and remote branch/tag enumeration by endpoint and require exact commit tips with side-effect-free remote reads.
- `named-remote-branch-tracking`: Expose lookup of the configured upstream for an arbitrary local branch while keeping upstream metadata branch-only.

## Impact

- Public API signatures and payloads in `src/daggerml/_core/dml.py`.
- Ref and upstream access in `src/daggerml/_core/head.py`, remote ref enumeration in `src/daggerml/_core/remote.py`, and launch-state delegation to `ExecutionState`.
- Generated CLI/introspection metadata for the changed namespace methods.
- Contract tests for runtime inspection, all branch/tag source combinations, remote listing side effects, exact tip preservation, and arbitrary upstream lookup.
- Human-facing Python, history/remote, execution/runtime, and error documentation.
- No new external dependencies or persisted-data migration.
