## MODIFIED Requirements

### Requirement: Cancellation orphaning SHALL remove current-execution ownership under lock
When Phase 1 cancellation processes an execution ID, the runtime SHALL acquire the coordination lock for that execution's `cache_key`, retrying until acquired. If the execution has live callers, processing that ID SHALL stop without changing its lifecycle or active ownership. Otherwise, the runtime SHALL set lifecycle to `cancel-requested`, remove each direct caller/callee edge, and move `refs/active/<cache_key>.json` to `refs/cancel-targets/<execution_id>.json` while conditionally verifying that the active ref still names that execution. The move SHALL preserve the existing argv manifest without regeneration. Direct callees SHALL then be added to the Phase 1 work set.

#### Scenario: User cancellation starts with explicit execution IDs
- **WHEN** a user requests cancellation for execution IDs `e1` and `e2`
- **THEN** Phase 1 initializes its work set with exactly those IDs

#### Scenario: Live callers prevent cancellation planning
- **WHEN** Phase 1 pops `e1`
- **AND** `e1` still has live callers
- **THEN** Phase 1 stops processing `e1`
- **AND** it does not mark `e1` cancel-requested

#### Scenario: Orphaned execution moves active ownership to a cancel target
- **WHEN** Phase 1 processes orphaned execution `e1` for cache key `ck1`
- **AND** `active/ck1` names `e1`
- **THEN** it marks `e1` as `cancel-requested`
- **AND** it removes `e1` as caller from each direct callee before planning those callees
- **AND** it moves the existing active manifest to `cancel-targets/e1`
- **AND** it does not regenerate the argv manifest

#### Scenario: Active ref rebinding is not overwritten
- **WHEN** Phase 1 processes `e1`
- **AND** `active/ck1` names a different execution
- **THEN** it does not move or delete that active ref

### Requirement: Cancellation Phase 1 SHALL not invoke adapters
Phase 1 SHALL only plan cancellation, update lifecycle state, move active ownership, and enqueue direct callees. It SHALL perform no adapter invocation.

#### Scenario: Planning completes without adapter work
- **WHEN** Phase 1 processes a cancellation work set
- **THEN** no invoke or cancel adapter operation is sent

### Requirement: Cancellation Phase 2 SHALL be distributed and leaves-first
Each runtime handling a `cancel-requested` execution SHALL wait for its direct callees to reach `cancel-ready`, invoke cancellation for those callees using their cancel-target refs, persist those callees as `canceled`, and then persist its own execution as `cancel-ready`. The wait SHALL time out after 60 seconds, after which the runtime SHALL perform the cancel-adapter work anyway.

#### Scenario: Parent waits for callees
- **WHEN** a cancel-requested execution has a callee that is not `cancel-ready`
- **THEN** its runtime does not yet invoke that callee's cancel adapter

#### Scenario: Leaf-first cleanup advances the parent
- **WHEN** all direct callees of `e1` are `cancel-ready`
- **THEN** the runtime invokes their cancel adapters
- **AND** it marks those callees `canceled`
- **AND** it marks `e1` `cancel-ready`

#### Scenario: Readiness timeout forces cleanup
- **WHEN** an execution remains `cancel-ready` for more than 60 seconds without normal handoff cleanup
- **THEN** a runtime invokes the applicable cancel adapters anyway
- **AND** the cleanup path remains safe to retry
