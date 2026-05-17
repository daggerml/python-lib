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
The runtime SHALL treat `exec/edges/<callee_execution_id>/<caller_execution_id>.json` as a live caller edge owned by the caller runtime. The caller runtime that created the edge SHALL be allowed to remove that edge when it cancels or otherwise stops being a caller of the callee execution.

#### Scenario: Caller cancellation removes its own live edge
- **WHEN** caller execution `e0` is cancelled after creating edge `exec/edges/e1/e0.json`
- **THEN** the runtime handling `e0` cancellation SHALL be allowed to remove that edge

#### Scenario: Other callers preserve callee liveness
- **WHEN** caller `e0` removes its edge to callee `e1`
- **AND** another live edge for `e1` still exists
- **THEN** the runtime SHALL continue to treat `e1` as having live callers

### Requirement: Live caller edges and spawned execution ids SHALL remain distinct
The runtime SHALL use live caller edges for reverse-lineage invalidation and orphan detection, and SHALL use `execution_record.spawned_execution_ids` for cancellation traversal. Removal of a live caller edge SHALL NOT remove the callee from the caller's historical spawned execution summary.

#### Scenario: Removing live edge preserves historical cancellation dependency
- **WHEN** caller `e0` removes its live edge to callee `e1` during cancellation
- **THEN** `e1` MAY still remain in `e0`'s `spawned_execution_ids`
- **AND** the runtime SHALL continue treating those structures as distinct sources of truth
