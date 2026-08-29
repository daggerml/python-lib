## MODIFIED Requirements

### Requirement: Runtime SHALL maintain one mutable execution record per execution id
The runtime SHALL persist one mutable lifecycle object per execution id as `execution_record`, separate from caller-owned `launch_state`. `execution_record` SHALL include `execution_id`, `cache_key`, `lifecycle`, `updated_at`, `created_at`, `spawned_execution_ids`, `child_execution_ids`, and `cancellation_requested_by`, where `cancellation_requested_by` is `str | null`. `lifecycle` SHALL be one of `pending`, `running`, `cancel-requested`, `cancel-ready`, `canceled`, `succeeded`, or `failed`. `spawned_execution_ids` SHALL be the deduped set of active direct child execution ids still in flight for cancellation traversal. `child_execution_ids` SHALL be the deduped set of completed direct child execution ids retained for lineage monitoring. Direct canceled descendants SHALL NOT be moved into `child_execution_ids`. Execution-record updates SHALL use compare-and-swap with the latest known ETag.

#### Scenario: Cancellation planning records cancel-requested
- **WHEN** Phase 1 claims orphaned execution `e1`
- **THEN** `exec/state/e1.json` stores `lifecycle = "cancel-requested"`

#### Scenario: Cancel-ready remains runtime-owned
- **WHEN** an execution's descendant cleanup reaches the handoff point
- **THEN** its execution record stores `lifecycle = "cancel-ready"`
- **AND** the adapter response does not write that lifecycle directly

#### Scenario: Canceled descendants remain out of completed lineage
- **WHEN** a direct child reaches `canceled`
- **THEN** it is removed from active spawned tracking as appropriate
- **AND** it is not added to `child_execution_ids`

### Requirement: Runtime SHALL separate caller-owned launch state from runtime-owned lifecycle state
The runtime SHALL treat `launch_state` as caller-owned state for invocation and resume, and `execution_record` as execution-runtime-owned state for lifecycle, spawned execution summaries, and cancellation metadata. Cancellation Phase 1 SHALL own planning updates under the cache-key lock. Cancellation Phase 2 SHALL allow distributed runtimes to make only the lifecycle transitions defined by the cancellation protocol.

#### Scenario: Phase 1 updates lifecycle under the cache-key lock
- **WHEN** Phase 1 marks `e1` cancel-requested
- **THEN** it holds the lock for `e1`'s cache key while making that update

#### Scenario: Phase 2 owns cleanup lifecycle transitions
- **WHEN** descendant cleanup completes for `e1`
- **THEN** the runtime may persist `e1` as `cancel-ready` or `canceled` according to the Phase 2 protocol
- **AND** an adapter cannot persist those execution-record values itself
