## Purpose
Define durable caller/callee lineage and registration cleanup for runtime executions.

## Requirements

### Requirement: Call-edge records SHALL represent realized rooted dependencies
The runtime SHALL record only realized rooted dependencies. An edge SHALL mean that caller id `caller_execution_id` was observed to depend on callee execution `callee_execution_id` during runtime execution, even if that dependency is discovered during a later `start_fn` poll cycle. The caller id MAY be either a normal execution id or a synthetic root index id.

#### Scenario: Dependency discovered after initial launch still creates edge
- **WHEN** execution `e0` does not know about callee `e1` on its first poll but discovers that dependency on a later poll
- **THEN** the runtime SHALL create the edge record for `e1 <- e0` when that dependency becomes known

#### Scenario: Repeated observation does not require a second edge fact
- **WHEN** execution `e0` rediscovers an existing dependency on `e1`
- **THEN** the runtime SHALL continue to treat `e1 <- e0` as one canonical edge fact

#### Scenario: Index root creates rooted dependency edge
- **WHEN** index `idx1` starts execution `e1`
- **THEN** the runtime SHALL treat `e1 <- idx1` as one canonical rooted edge fact

### Requirement: Runtime SHALL persist canonical edge records by callee execution id
The runtime SHALL persist each rooted dependency as the immutable object `exec/edges/<callee_execution_id>/<caller_execution_id>.json`. The payload SHALL include only `caller_execution_id` and `callee_execution_id`.

#### Scenario: Edge record is written at canonical path
- **WHEN** execution `e0` discovers a dependency on execution `e1`
- **THEN** the runtime SHALL write `exec/edges/e1/e0.json`
- **AND** that object SHALL contain JSON with `caller_execution_id = "e0"` and `callee_execution_id = "e1"`

#### Scenario: Reverse lineage query lists callers by callee execution id
- **WHEN** an invalidation planner needs all callers of execution `e1`
- **THEN** it SHALL obtain them by reading the objects under `exec/edges/e1/`

#### Scenario: Index root uses the same canonical edge namespace
- **WHEN** index `idx1` starts execution `e1`
- **THEN** the runtime SHALL write `exec/edges/e1/idx1.json`
- **AND** that object SHALL contain JSON with `caller_execution_id = "idx1"` and `callee_execution_id = "e1"`

### Requirement: Live caller edges SHALL be caller-owned and removable
The runtime SHALL treat `exec/edges/<callee_execution_id>/<caller_execution_id>.json` as a live caller reference owned by the caller runtime. A caller edge SHALL become valid only after registration has serialized with the callee lifecycle and confirmed that the callee is neither `cancel-pending` nor `canceled`. The caller runtime that created the edge SHALL remove that edge when cancellation selects the caller or when registration fails. Cancellation planning SHALL preserve a callee that retains any valid incoming caller edge.

#### Scenario: Caller cancellation removes its own live edge
- **WHEN** Phase 1 selects caller execution `e0` after it created `exec/edges/e1/e0.json`
- **THEN** Phase 1 SHALL idempotently remove that edge before evaluating `e1`

#### Scenario: Other callers preserve callee liveness
- **WHEN** caller `e0` removes its edge to callee `e1`
- **AND** another valid live edge for `e1` still exists
- **THEN** cancellation planning SHALL leave `e1` active

#### Scenario: Incomplete registration does not preserve callee liveness
- **WHEN** registration creates an edge but cannot validate the callee lifecycle or complete caller lineage registration
- **THEN** the registering caller SHALL remove its incomplete edge
- **AND** that edge SHALL NOT be treated as a durable valid caller reference

### Requirement: Caller registration SHALL serialize with cancellation selection
Caller registration and Phase 1 cancellation selection SHALL use the callee execution's coordination boundary to order edge publication against the lifecycle decision. If registration wins, cancellation SHALL observe the valid edge before selecting the callee. If cancellation wins, registration SHALL observe `cancel-pending` or `canceled`, remove any incomplete edge, and SHALL NOT invoke the callee adapter.

#### Scenario: Registration wins the race
- **WHEN** caller registration completes while the callee remains active
- **THEN** a concurrent cancellation planner SHALL observe the valid caller edge
- **AND** it SHALL NOT select the callee while that reference remains

#### Scenario: Cancellation wins the race
- **WHEN** Phase 1 compare-and-swaps the callee to `cancel-pending` before caller registration completes
- **THEN** registration SHALL fail without invoking the callee adapter
- **AND** it SHALL remove any incomplete edge it owns

### Requirement: Live caller edges and spawned execution ids SHALL remain distinct
The runtime SHALL retain separate caller-edge objects for reverse lineage and orphan detection. It SHALL update `spawned_execution_ids` and `child_execution_ids` only in the caller's unified execution record while holding that record's embedded lock. Edge removal SHALL NOT erase historical forward summaries.

#### Scenario: Caller summary mutation is locked
- **WHEN** caller `e0` registers or completes child `e1`
- **THEN** the runtime holds the lock for `execution/e0` while updating its forward summaries

#### Scenario: Edge removal preserves summary
- **WHEN** the live edge `e1 <- e0` is removed during cancelation
- **THEN** `e1` MAY remain in `e0`'s spawned execution summary

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
