## MODIFIED Requirements

### Requirement: Failed child registration SHALL roll back unrealized caller edges
When a launch writes a caller edge but fails to register the child in the caller's locked execution record, it SHALL remove that edge before surfacing failure. If the launch created a fresh execution but lost or failed cache-pointer publication, it SHALL conditionally delete only its unchanged execution record. Reused current executions and their cache pointers SHALL remain intact.

#### Scenario: Fresh registration failure cleans owned artifacts
- **WHEN** fresh execution `e1` cannot be registered under caller `e0`
- **THEN** the runtime removes edge `e1 <- e0`
- **AND** it conditionally removes only fresh artifacts still owned by that launch

#### Scenario: Reused execution survives registration failure
- **WHEN** registration fails after resolving shared current execution `e1`
- **THEN** the runtime removes only the attempted caller edge
- **AND** it preserves `execution/e1` and its cache pointer

### Requirement: Live caller edges and spawned execution ids SHALL remain distinct
The runtime SHALL retain separate caller-edge objects for reverse lineage and orphan detection. It SHALL update `spawned_execution_ids` and `child_execution_ids` only in the caller's unified execution record while holding that record's embedded lock. Edge removal SHALL NOT erase historical forward summaries.

#### Scenario: Caller summary mutation is locked
- **WHEN** caller `e0` registers or completes child `e1`
- **THEN** the runtime holds the lock for `execution/e0` while updating its forward summaries

#### Scenario: Edge removal preserves summary
- **WHEN** the live edge `e1 <- e0` is removed during cancelation
- **THEN** `e1` MAY remain in `e0`'s spawned execution summary
