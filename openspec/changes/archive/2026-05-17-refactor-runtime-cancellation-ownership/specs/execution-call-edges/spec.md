## ADDED Requirements

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
