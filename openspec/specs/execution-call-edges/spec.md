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
The runtime SHALL treat `exec/edges/<callee_execution_id>/<caller_execution_id>.json` as a live caller reference owned by the caller runtime. A caller edge SHALL become valid only after registration has serialized with the callee lifecycle, confirmed that the callee is neither `cancel-pending` nor `canceled`, and completed the caller's spawned summary. The caller runtime that created the edge SHALL remove that edge when cancellation selects the caller or when registration fails. Cancellation planning SHALL preserve a `pending` or `running` callee that retains any valid incoming caller edge. An execution already in `cancel-pending` SHALL remain selected for idempotent planning recovery because no new edge can become valid after that lifecycle was persisted.

#### Scenario: Caller cancellation removes its own live edge
- **WHEN** Phase 1 selects caller execution `e0` after it created `exec/edges/e1/e0.json`
- **THEN** Phase 1 SHALL idempotently remove that edge before evaluating active `e1`

#### Scenario: Other callers preserve active callee liveness
- **WHEN** caller `e0` removes its edge to active callee `e1`
- **AND** another valid live edge for `e1` still exists
- **THEN** cancellation planning SHALL leave `e1` active

#### Scenario: Incomplete registration does not preserve callee liveness
- **WHEN** registration creates an edge but cannot validate the callee lifecycle or complete caller lineage registration
- **THEN** the registering caller SHALL remove its incomplete edge
- **AND** that edge SHALL NOT be treated as a durable valid caller reference

#### Scenario: Selected execution resumes cleanup
- **WHEN** planning rereads lifecycle `cancel-pending`
- **THEN** it SHALL retain that execution in the selected set
- **AND** it SHALL resume idempotent outgoing-edge and cache-pointer cleanup

### Requirement: Caller registration SHALL serialize with cancellation selection
Caller registration and Phase 1 cancellation selection SHALL acquire the callee's driver lock to order edge publication against the callee lifecycle decision, while the caller forward summary SHALL use guarded lock-free state CAS. Registration SHALL publish the canonical caller edge and successfully add the callee to the caller's `spawned_execution_ids` while the caller remains `running` before invoking the adapter. If registration wins, cancellation SHALL observe the valid caller reference before selecting the callee. If callee cancellation wins, or caller summary registration observes a caller lifecycle other than `running`, registration SHALL remove its incomplete edge and SHALL NOT invoke the adapter.

#### Scenario: Registration wins the race
- **WHEN** registration publishes an edge and completes guarded caller lineage bookkeeping while holding the callee driver lock
- **THEN** concurrent cancellation SHALL observe the valid caller reference
- **AND** adapter invocation MAY proceed

#### Scenario: Callee cancellation wins the race
- **WHEN** cancellation stores `cancel-pending` for the callee before registration validates callee state
- **THEN** registration SHALL remove its incomplete edge
- **AND** it SHALL NOT invoke the adapter

#### Scenario: Caller cancellation wins spawned registration
- **WHEN** registration attempts to add the callee after the caller is no longer `running`
- **THEN** the caller summary CAS SHALL reject the update
- **AND** registration SHALL remove its incomplete edge and SHALL NOT invoke the adapter

### Requirement: Live caller edges and spawned execution ids SHALL remain distinct
The runtime SHALL retain separate caller-edge objects for reverse lineage and orphan detection. It SHALL update `spawned_execution_ids` and `child_execution_ids` in the caller's `state.json` through guarded CAS with bounded retry. Edge removal SHALL NOT erase historical forward summaries.

#### Scenario: Caller summary mutation uses guarded state CAS
- **WHEN** caller `e0` registers or completes child `e1`
- **THEN** it conditionally updates the latest semantic state without requiring the driver lock

#### Scenario: Caller summary mutation is locked
- **WHEN** caller `e0` registers or completes child `e1`
- **THEN** the state-object CAS serializes its forward-summary update without the driver lock

#### Scenario: Edge removal preserves summary
- **WHEN** the live edge `e1 <- e0` is removed during cancelation
- **THEN** `e1` may remain in `e0`'s spawned execution summary

### Requirement: Failed child registration SHALL roll back unrealized caller edges
When a launch writes `exec/edges/<child>/<caller>.json` but fails to register the child by CAS-updating the caller's `state.json` lineage arrays, it SHALL remove that edge before surfacing failure and SHALL NOT call the adapter. If that launch created a fresh execution, it SHALL conditionally remove its matching cache pointer and only its unchanged owned `metadata.json`, `state.json`, and `driver.json` objects. Reused current executions, their split records, uploaded content-addressed argument objects, and their cache pointers SHALL remain intact. Because rejected registration never invokes the adapter, it SHALL NOT require scratch-prefix cleanup.

#### Scenario: Fresh registration failure cleans owned artifacts
- **WHEN** fresh execution `e1` cannot be registered in caller `e0`'s `state.json`
- **THEN** the runtime SHALL remove `exec/edges/e1/e0.json`
- **AND** it SHALL conditionally remove the matching cache pointer and unchanged split artifacts still owned by that launch
- **AND** it SHALL NOT invoke the adapter

#### Scenario: Reused execution survives registration failure
- **WHEN** registration fails after resolving shared current execution `e1`
- **THEN** the runtime SHALL remove only attempted edge `exec/edges/e1/e0.json`
- **AND** it SHALL preserve all `exec/execution/e1/` split files and its cache pointer
- **AND** it SHALL NOT invoke the adapter

#### Scenario: Shared argument objects survive fresh cleanup
- **WHEN** failed fresh registration uploaded content-addressed argument objects before reservation
- **THEN** cleanup SHALL preserve those shared objects

### Requirement: Terminal-child summaries SHALL converge during cancellation
When a direct child reaches normal terminal lifecycle, the runtime SHALL CAS-move its ID from the caller's `spawned_execution_ids` to `child_execution_ids`. This bookkeeping SHALL occur for a `running` or `cancel-pending` caller without the caller driver lock. A `cancel-pending` caller SHALL retain its cancellation lifecycle and SHALL observe cancellation after the lineage update. Canceled children SHALL remain in `spawned_execution_ids`.

#### Scenario: Running caller records terminal child
- **WHEN** a normally terminal child completes under a running caller
- **THEN** the caller SHALL remove it from spawned lineage and add it to completed lineage

#### Scenario: Cancel-pending caller records terminal child
- **WHEN** a normally terminal child completes after its caller becomes `cancel-pending`
- **THEN** the caller SHALL persist the same lineage move without changing lifecycle
- **AND** the call path SHALL then surface cancellation

#### Scenario: Canceled child remains spawned
- **WHEN** a direct child reaches `canceled`
- **THEN** it SHALL remain in the caller's `spawned_execution_ids`
- **AND** it SHALL NOT be added to `child_execution_ids`
