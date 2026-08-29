## MODIFIED Requirements

### Requirement: Failed child registration SHALL roll back unrealized caller edges
When a launch writes `exec/edges/<child>/<caller>.json` but fails to register the child by CAS-updating the caller's `state.json` lineage arrays, it SHALL remove that edge before surfacing failure. If the launch created a fresh execution but lost or failed cache-pointer publication, it SHALL conditionally delete only its unchanged owned `metadata.json`, `state.json`, and `driver.json` objects. Reused current executions, their split records, and their cache pointers SHALL remain intact. No singular edge path or unified execution object SHALL be consulted.

#### Scenario: Fresh registration failure cleans owned artifacts
- **WHEN** fresh execution `e1` cannot be registered in caller `e0`'s `state.json`
- **THEN** the runtime removes `exec/edges/e1/e0.json`
- **AND** it conditionally removes only unchanged split artifacts still owned by that launch

#### Scenario: Reused execution survives registration failure
- **WHEN** registration fails after resolving shared current execution `e1`
- **THEN** the runtime removes only attempted edge `exec/edges/e1/e0.json`
- **AND** it preserves all `exec/execution/e1/` split files and its cache pointer
