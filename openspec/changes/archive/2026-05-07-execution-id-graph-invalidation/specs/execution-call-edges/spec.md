## REMOVED Requirements

### Requirement: Runtime SHALL distinguish user-dags from fn-dags for call-edge tracking
**Reason**: The execution graph now uses execution ids as the only lineage node identity.
**Migration**: Remove cache-key and index-based lineage writes entirely and derive graph roots only from cache refs and execution state objects. No backward-compatible reads or writes to the prior lineage layout are supported, and nothing reads from `calls/from/...` or `calls/to/...`.

### Requirement: Runtime SHALL persist forward call-edge indexes by caller type
**Reason**: Forward lineage is no longer stored as mutable cache-key arrays segmented by caller type.
**Migration**: Replace prior forward lineage storage with canonical execution-edge records at `exec/edges/<callee_execution_id>/<caller_execution_id>.json`. No backward-compatible forward lineage path remains, and `calls/from/...` is never read.

### Requirement: Runtime SHALL persist reverse call-edge indexes for callee cache keys
**Reason**: Reverse lineage is no longer stored as shared mutable cache-key index objects.
**Migration**: Replace prior reverse lineage reads with `exec/edges/<callee_execution_id>/`. No backward-compatible reverse lineage path remains, and `calls/to/...` is never read.

### Requirement: Call-edge index updates SHALL be concurrency-safe and canonicalized
**Reason**: Edge records become immutable create-once objects rather than merge-updated JSON indexes.
**Migration**: Replace prior merge-updated lineage objects with idempotent create semantics for canonical edge-object paths. No backward-compatible lineage-update path remains, and no code path consults `calls/from/...` or `calls/to/...`.

## MODIFIED Requirements

### Requirement: Call-edge records SHALL represent realized execution dependencies
The runtime SHALL record only realized execution dependencies. An edge SHALL mean that caller execution `caller_execution_id` was observed to depend on callee execution `callee_execution_id` during runtime execution, even if that dependency is discovered during a later `start_fn` poll cycle.

#### Scenario: Dependency discovered after initial launch still creates edge
- **WHEN** execution `e0` does not know about callee `e1` on its first poll but discovers that dependency on a later poll
- **THEN** the runtime SHALL create the edge record for `e1 <- e0` when that dependency becomes known

#### Scenario: Repeated observation does not require a second edge fact
- **WHEN** execution `e0` rediscovers an existing dependency on `e1`
- **THEN** the runtime SHALL continue to treat `e1 <- e0` as one canonical edge fact

### Requirement: Runtime SHALL persist canonical edge records by callee execution id
The runtime SHALL persist each execution dependency as the immutable object `exec/edges/<callee_execution_id>/<caller_execution_id>.json`. The payload SHALL include only `caller_execution_id` and `callee_execution_id`.

#### Scenario: Edge record is written at canonical path
- **WHEN** execution `e0` discovers a dependency on execution `e1`
- **THEN** the runtime SHALL write `exec/edges/e1/e0.json`
- **AND** that object SHALL contain JSON with `caller_execution_id = "e0"` and `callee_execution_id = "e1"`

#### Scenario: Reverse lineage query lists callers by callee execution id
- **WHEN** an invalidation planner needs all callers of execution `e1`
- **THEN** it SHALL obtain them by reading the objects under `exec/edges/e1/`
