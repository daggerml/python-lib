## MODIFIED Requirements

### Requirement: Runtime SHALL maintain one mutable execution record per execution id
The runtime SHALL persist one mutable lifecycle object per execution id as `execution_record`, separate from caller-owned `launch_state`. `execution_record` SHALL include `execution_id`, `cache_key`, `lifecycle`, `updated_at`, `created_at`, `spawned_execution_ids`, `child_execution_ids`, and `cancellation_requested_by`, where `cancellation_requested_by` is `str | null`. `lifecycle` SHALL be one of `running`, `cancel-pending`, `cancel-ready`, `canceled`, `succeeded`, or `failed`.

`spawned_execution_ids` SHALL be the deduped set of active direct child execution ids still in flight for cancellation traversal. `child_execution_ids` SHALL be the deduped set of completed direct child execution ids retained for lineage monitoring. Direct canceled descendants SHALL NOT be moved into `child_execution_ids`.

The same `execution_record` schema SHALL also be used for each live index id. For index-root records, the object path SHALL be `exec/state/<index_id>.json`, `execution_id` SHALL equal the `index_id`, and `cache_key` MAY be `null` when the root has no lock-bearing execution cache identity.

#### Scenario: Cancel-ready is a runtime-owned intermediate lifecycle
- **WHEN** runtime cancellation has finished driving direct spawned descendants for execution `e1`
- **THEN** the runtime SHALL persist `exec/state/e1.json` with `lifecycle = "cancel-ready"` before any parent adapter cancel step treats `e1` as ready

#### Scenario: Full cancel writes terminal canceled after F2
- **WHEN** `runtime.cancel(e1, mode="full")` finishes its F2 driver
- **THEN** the runtime SHALL persist `exec/state/e1.json` with `lifecycle = "canceled"`

#### Scenario: Canceled direct child does not move into child lineage
- **WHEN** caller execution `e0` has direct child `e1` in `spawned_execution_ids`
- **AND** `e1` reaches lifecycle `canceled`
- **THEN** the runtime SHALL remove `e1` from `e0`'s active cancellation planning as needed
- **BUT** it SHALL NOT add `e1` to `e0`'s `child_execution_ids`

#### Scenario: Root execution record may be lockless
- **WHEN** a user-root execution record represents an index root with no execution cache key
- **THEN** `exec/state/<index_id>.json` MAY record `cache_key = null`

### Requirement: Adapter cancel dispatch SHALL target direct children that are cancel-ready
The runtime SHALL dispatch adapter cancellation only for direct spawned child executions of the current execution, and only after the target child's execution record has reached `lifecycle = "cancel-ready"`. Runtime lifecycle ownership remains outside the adapter response contract.

#### Scenario: Parent waits for child cancel-ready before adapter cancel dispatch
- **WHEN** execution `e0` is driving cancellation for direct child `e1`
- **AND** `exec/state/e1.json` is still `cancel-pending`
- **THEN** `F2(e0)` SHALL NOT invoke adapter cancellation for `e1` yet

#### Scenario: Adapter cancel response does not define execution-record lifecycle names
- **WHEN** adapter cancellation is invoked for execution `e1`
- **THEN** the adapter response contract SHALL remain separate from execution-record-only lifecycle values such as `cancel-ready`
