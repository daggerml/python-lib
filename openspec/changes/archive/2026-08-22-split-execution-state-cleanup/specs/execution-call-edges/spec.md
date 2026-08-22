## MODIFIED Requirements

### Requirement: Caller registration SHALL serialize with cancellation selection
Caller registration and Phase 1 cancellation selection SHALL acquire the callee's driver lock to order edge publication against the lifecycle decision, while lifecycle changes themselves SHALL use guarded state CAS. If registration wins, cancellation SHALL observe the valid edge before selecting the callee. If cancellation wins, registration SHALL observe `cancel-pending` or `canceled`, remove any incomplete edge, and SHALL NOT invoke the callee adapter.

#### Scenario: Registration wins the race
- **WHEN** registration publishes an edge and completes guarded state bookkeeping while holding the callee driver lock
- **THEN** concurrent cancellation observes the valid caller reference

#### Scenario: Cancellation wins the race
- **WHEN** cancellation stores cancel-pending before registration validates state
- **THEN** registration removes its incomplete edge and does not invoke the adapter

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
