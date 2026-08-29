## MODIFIED Requirements

### Requirement: Manual cancellation SHALL support `full` and `drive` runtime modes
Manual cancellation SHALL execute as two retryable phases. In `full` mode, Phase 1 SHALL determine the complete reachable cancellation set by selecting active executions with no valid caller references, persisting cancellation requester metadata, compare-and-swapping each selected lifecycle to `cancel-pending`, conditionally deleting matching cache pointers, and removing selected executions' outgoing caller edges. Phase 1 SHALL complete without invoking adapters. Phase 2 SHALL invoke cancellation for every selected execution and compare-and-swap each lifecycle directly from `cancel-pending` to `canceled`. In `drive` mode, the runtime SHALL reconstruct and resume cancellation from persisted `cancel-pending` state. Both phases SHALL hold the applicable execution coordination lock for lifecycle mutations and SHALL retry CAS conflicts from fresh state. The persisted field name SHALL remain `cancelation`.

#### Scenario: Cancelation removes current cache binding
- **WHEN** Phase 1 selects current execution `e1` as `cancel-pending`
- **THEN** it SHALL conditionally delete `cache/ck1` if that pointer still contains `e1`

#### Scenario: Cancelation uses stored argv
- **WHEN** Phase 2 invokes cancelation for `e1`
- **THEN** it SHALL read `argv_ref` from `execution/e1`
- **AND** it SHALL not require an active or cancel-target ref

#### Scenario: Full mode separates planning from adapter work
- **WHEN** full cancellation selects multiple reachable executions
- **THEN** it SHALL finish Phase 1 for the complete selected set before invoking any cancel adapter

#### Scenario: Drive mode resumes persisted work
- **WHEN** drive mode starts from an execution whose lifecycle is `cancel-pending`
- **THEN** it SHALL reconstruct selected descendants from execution records
- **AND** it SHALL perform idempotent planning cleanup and Phase 2 adapter work

#### Scenario: Lifecycle changes require ownership and CAS
- **WHEN** either phase changes an execution lifecycle
- **THEN** it SHALL hold that execution's matching coordination owner
- **AND** it SHALL use compare-and-swap against fresh execution state
