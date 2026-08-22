## MODIFIED Requirements

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

## ADDED Requirements

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
