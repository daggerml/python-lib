## ADDED Requirements

### Requirement: Manual cancellation SHALL run one resumable two-phase workflow
Manual cancellation SHALL first reconstruct and complete the reachable `cancel-pending` set without invoking adapters, then drive every selected execution to successful cancellation with bounded retries. Every invocation SHALL resume persisted `cancel-pending` work without a separate drive mode, and the persisted field name SHALL remain `cancelation`.

#### Scenario: Planning precedes adapter work
- **WHEN** cancellation selects multiple reachable executions
- **THEN** it SHALL finish Phase 1 for the complete selected set before invoking any cancel adapter

#### Scenario: Repeated call resumes persisted work
- **WHEN** cancellation starts from an execution already in `cancel-pending`
- **THEN** it SHALL reconstruct its selected descendants and run the same two phases

#### Scenario: Exhausted work remains resumable
- **WHEN** Phase 2 exhausts retries for an execution
- **THEN** that execution SHALL remain `cancel-pending` for a later cancellation call

## MODIFIED Requirements

### Requirement: Mutation-time cancellation rendezvous SHALL happen outside LMDB
When a local index mutation detects a non-active local index lifecycle, the mutation path SHALL release or abort its LMDB transaction before joining the single runtime cancellation workflow. A mutation path that sees a non-active local index SHALL use runtime cancellation as the synchronization point rather than requiring caller-managed thread or process handling.

#### Scenario: Inactive mutation gate joins drive mode outside LMDB
- **WHEN** a mutating index workflow sees local index lifecycle `inactive`
- **THEN** it SHALL abort or leave its LMDB transaction before calling `runtime.cancel(...)`
- **AND** it SHALL raise `_core.CancellationError` after that cancellation rendezvous returns

#### Scenario: Terminal local tombstone fails immediately
- **WHEN** a mutating index workflow sees local index lifecycle `canceled`
- **THEN** it SHALL abort or leave its LMDB transaction
- **AND** it SHALL raise `_core.CancellationError` without attempting cancellation again

## REMOVED Requirements

### Requirement: Manual cancellation SHALL support `full` and `drive` runtime modes
**Reason**: One resumable operation replaces mode-dependent planning and driving.

**Migration**: Call `runtime.cancel(...)`; it reconstructs persisted work and performs both phases.
