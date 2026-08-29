## MODIFIED Requirements

### Requirement: Live caller edges SHALL be caller-owned and removable
The runtime SHALL treat `exec/edges/<callee_execution_id>/<caller_execution_id>.json` as a caller-owned edge that remains relevant only while the caller is not cancelled. Cancellation SHALL mechanically remove the caller's edge objects for its direct callees.

Persisted edge objects are not a durable full history. Durable child history lives in `spawned_execution_ids`.

#### Scenario: Caller cancellation removes its direct edges
- **WHEN** caller execution `e0` is cancelled
- **THEN** the runtime handling `cancel(e0)` SHALL remove `exec/edges/<callee>/e0.json` for each direct callee recorded in `spawned_execution_ids`

#### Scenario: Cancelled callers are excluded from edge-based liveness
- **WHEN** caller execution `e0` has been cancelled
- **THEN** the system SHALL NOT continue treating `exec/edges/<callee>/e0.json` as live caller evidence

### Requirement: Spawned execution ids SHALL remain the historical child summary
`spawned_execution_ids` SHALL remain the historical summary of children started by an execution even after cancellation removes the corresponding live caller edges.

#### Scenario: Edge removal preserves historical child summary
- **WHEN** cancellation of `e0` removes `exec/edges/e1/e0.json`
- **THEN** `exec/state/e0.json` MAY still retain `e1` in `spawned_execution_ids`
