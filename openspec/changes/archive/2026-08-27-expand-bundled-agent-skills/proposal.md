## Why

The bundled agent skills are too compressed and their current boundaries blur data traversal, runtime/cache operations, and extension development. Agents need four independently useful guides aligned with the actual jobs of querying data, authoring DAGs, managing repositories, and building integrations.

## What Changes

- **BREAKING**: Replace the bundled `inspection` skill and its `dml skills inspection` export with a `querying` skill focused on extracting data, traversing committed and in-progress DAGs, following provenance, and capturing persisted errors.
- Expand `authoring` guidance to prefer passing nodes, projections, and call results directly between funks, with a concrete `.value()` example showing materialization only at a Python computation boundary.
- Expand `repository` guidance to cover project setup, configuration, history, remotes, dependencies, garbage collection, and cache inspection, validation, and intentional invalidation.
- Add an `extensions` skill covering adapters, executors, codecs, plugin registration, lifecycle contracts, and extension testing.
- Require exactly four bundled skills and raise the per-skill size limit from 250 to 1000 words so each export can remain self-contained before later editorial pruning.
- Update the Python/CLI export surface, package resources, user documentation, and contract tests for the new skill set and responsibilities.

## Capabilities

### New Capabilities

None.

### Modified Capabilities

- `bundled-agent-skills`: Change the required skill set, topic boundaries, authoring materialization guidance, and maximum document size.
- `admin-cli-controls`: Replace the `inspection` skill command with `querying` and expose the new `extensions` command.

## Impact

- Bundled resources under `src/daggerml/_core/skills/`.
- `Dml.skills` and generated `dml skills` commands.
- Agent-skill contract tests and package-resource coverage.
- User-facing CLI and Python API documentation describing bundled skills.
- Existing consumers of `inspection` must switch to `querying`.
