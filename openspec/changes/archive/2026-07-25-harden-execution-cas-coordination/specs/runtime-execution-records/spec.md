## ADDED Requirements

### Requirement: Runtime SHALL durably register a child before adapter invocation
For an adapter-backed child execution, the runtime SHALL append the child execution ID to the caller's `spawned_execution_ids` through a successful compare-and-swap update before invoking the adapter. The runtime SHALL retry CAS conflicts with bounded backoff. If registration exhausts its retry budget, the runtime SHALL fail the launch and SHALL NOT invoke the adapter.

#### Scenario: Cancellation update wins child-registration contention
- **WHEN** cancellation persists a non-running lifecycle on caller `e0` before child `e1` registration can update `e0`
- **THEN** registration of `e1` SHALL fail
- **AND** the runtime SHALL NOT invoke `e1`'s adapter

#### Scenario: Child registration wins cancellation contention
- **WHEN** registration persists `e1` in `e0`'s `spawned_execution_ids` before cancellation updates `e0`
- **THEN** cancellation planning SHALL reread `e0`
- **AND** it SHALL include `e1` in its direct-descendant traversal

#### Scenario: Child registration exhausts retries
- **WHEN** child `e1` cannot append to caller `e0` after the bounded CAS retry budget
- **THEN** the runtime SHALL raise a coordination failure
- **AND** it SHALL NOT invoke `e1`'s adapter

### Requirement: Runtime SHALL preserve uncompleted and terminal child lineage distinctly
`spawned_execution_ids` SHALL contain deduped direct children that have not reached normal terminal completion. `child_execution_ids` SHALL contain deduped direct children that reached `succeeded` or `failed`. A child with lifecycle `canceled` SHALL remain in `spawned_execution_ids` and SHALL NOT be added to `child_execution_ids`. The two lists SHALL remain disjoint.

#### Scenario: Terminal child moves to completed lineage
- **WHEN** direct child `e1` of caller `e0` reaches lifecycle `succeeded` or `failed`
- **THEN** the runtime SHALL remove `e1` from `e0`'s `spawned_execution_ids`
- **AND** it SHALL add `e1` to `e0`'s `child_execution_ids`

#### Scenario: Canceled child remains uncompleted lineage
- **WHEN** direct child `e1` of caller `e0` reaches lifecycle `canceled`
- **THEN** `e1` SHALL remain in `e0`'s `spawned_execution_ids`
- **AND** `e1` SHALL not appear in `e0`'s `child_execution_ids`

#### Scenario: Canceled child satisfies cancellation driving
- **WHEN** caller `e0` drives cancellation and direct child `e1` is `canceled`
- **THEN** `e0` SHALL treat `e1` as satisfied for descendant cleanup
- **AND** it SHALL not remove `e1` from `spawned_execution_ids`

### Requirement: Runtime SHALL surface terminal-child bookkeeping exhaustion
The runtime SHALL move a normally terminal direct child from `spawned_execution_ids` to `child_execution_ids` through a compare-and-swap update with bounded backoff. If that update exhausts its retry budget, the runtime SHALL surface a coordination failure and SHALL preserve state needed for a later terminal poll to retry the update.

#### Scenario: Terminal-child bookkeeping exhausts retries
- **WHEN** caller `e0` cannot record terminal child `e1` after the bounded CAS retry budget
- **THEN** the runtime SHALL surface a coordination failure
- **AND** it SHALL not silently report bookkeeping success
