## Why

`Dag.result` currently treats a failed committed DAG as unfinished because it only looks for a result node. A committed DAG with a terminal error should expose that failure to callers rather than report an inaccurate lifecycle state.

## What Changes

- Make `daggerml.api.Dag.result` raise the DAG's persisted `Error` when the committed DAG has a terminal error.
- Preserve the existing result-node return for successful DAGs and the existing error for DAGs with neither terminal result nor error.
- Document failed terminal DAG result access in the public authoring and error guidance.
- Add contract and persisted-workflow integration coverage for failed terminal DAG result access.

## Capabilities

### New Capabilities

- None.

### Modified Capabilities

- `node-error-inspection`: Define public `Dag.result` behavior for committed DAGs with terminal errors.

## Impact

- Affected public API: `daggerml.api.Dag.result`.
- Affected code: `src/daggerml/api.py`, API contract tests, and API integration tests.
- Affected documentation: public Python authoring and error behavior documentation.
- No storage format, dependency, or CLI changes.
