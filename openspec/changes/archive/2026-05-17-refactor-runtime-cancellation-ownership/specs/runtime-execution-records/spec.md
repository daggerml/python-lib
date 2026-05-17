## MODIFIED Requirements

### Requirement: Runtime SHALL maintain one mutable execution object per execution id
The runtime SHALL persist one mutable lifecycle object per execution id as `execution_record`, separate from caller-owned `launch_state`. `execution_record` SHALL include `execution_id`, `cache_key`, `lifecycle`, `updated_at`, `spawned_execution_ids`, and `cancellation_requested_by`, where `cancellation_requested_by` is `str | null`. `lifecycle` SHALL be one of `running`, `cancel-pending`, `cancel-detached`, `succeeded`, or `failed`. `spawned_execution_ids` SHALL be the deduped set of child execution ids started by that execution for cancellation traversal. `execution_record` updates SHALL use compare-and-swap with the latest known ETag. If a compare-and-swap update observes ETag drift, the runtime SHALL reread the record and SHALL raise cancellation interruption only when the reread lifecycle is already a `cancel-*` value; otherwise it SHALL continue from the latest valid reread state.

The same `execution_record` schema SHALL also be used for each live index id. For index-root records, the object path SHALL be `exec/state/<index_id>.json`, `execution_id` SHALL equal the `index_id`, `cache_key` SHALL equal the `index_id`, and `spawned_execution_ids` SHALL track the deduped set of execution ids started from that index.

The `execution_record` schema SHALL be:

- `execution_id: str`
- `cache_key: str`
- `lifecycle: "running" | "cancel-pending" | "cancel-detached" | "succeeded" | "failed"`
- `updated_at: int`
- `spawned_execution_ids: list[str]`
- `cancellation_requested_by: str | null`

#### Scenario: Index creation creates the initial execution record
- **WHEN** `IndexOps.create` initializes a new runtime root
- **THEN** it SHALL create an `execution_record` for that root before execution starts
- **AND** that record SHALL use `execution_id = index_id` and `cache_key = index_id`

#### Scenario: Lifecycle record does not store resume state
- **WHEN** the runtime persists `execution_record` for execution `e0`
- **THEN** it SHALL NOT store adapter resume state in that object
- **AND** resume state SHALL instead live only in caller-owned `launch_state`

#### Scenario: CAS reread continues on non-cancellation drift
- **WHEN** a compare-and-swap update for `execution_record` observes an ETag conflict
- **AND** the reread lifecycle is `running`, `succeeded`, or `failed`
- **THEN** the runtime SHALL continue from the reread record instead of raising cancellation interruption

#### Scenario: CAS reread raises on cancellation lifecycle drift
- **WHEN** a compare-and-swap update for `execution_record` observes an ETag conflict
- **AND** the reread lifecycle is `cancel-pending` or `cancel-detached`
- **THEN** the runtime SHALL surface cancellation interruption rather than continuing normal execution updates

#### Scenario: Root record accumulates spawned execution ids
- **WHEN** index `idx1` starts execution `e1`
- **THEN** the runtime SHALL update `exec/state/idx1.json` so that `spawned_execution_ids` contains `e1`

### Requirement: Adapter envelope and result schema SHALL follow the runtime-owned execution contract
The adapter envelope SHALL include `argv_ptr`, `cache_key`, `execution_id`, `remote`, `runnable`, `state`, `execution_status`, and `cancel_requested_by`. The adapter result SHALL use only `running`, `succeeded`, `failed`, or `cancel-detached` statuses. `running` MUST include durable `state`. `succeeded` MUST include `dag_id`. `failed` MUST include `error`. `cancel-detached` MUST identify a successful cancellation update that detached runtime ownership and MAY omit durable execution output.

#### Scenario: First adapter call uses null state
- **WHEN** the runtime invokes an adapter for a new execution
- **THEN** the adapter envelope SHALL include `state = null`

#### Scenario: Cancel update includes renamed cancellation lifecycle
- **WHEN** the runtime invokes an adapter for a cancel update
- **THEN** the adapter envelope SHALL include `execution_status = "cancel-pending"`
- **AND** it SHALL include `cancel_requested_by`

#### Scenario: Cancel update may return detached status
- **WHEN** an executor completes a cancel update successfully
- **THEN** the adapter result MAY use `status = "cancel-detached"`

#### Scenario: Pending is rejected
- **WHEN** an adapter returns `pending`
- **THEN** the runtime SHALL reject that result as invalid adapter output

## ADDED Requirements

### Requirement: Runtime SHALL separate caller-owned launch state from runtime-owned lifecycle state
The runtime SHALL treat `launch_state` as caller-owned state for launch and resume, and `execution_record` as execution-runtime-owned state for lifecycle, spawned execution summaries, and cancellation metadata. The caller runtime MAY transition a callee `execution_record` only to `cancel-pending` or `cancel-detached` during orphan-triggered cancellation, and SHALL NOT otherwise mutate lifecycle state owned by the callee execution runtime.

#### Scenario: Caller runtime owns launch state updates
- **WHEN** `start_fn` launches or resumes execution `e1`
- **THEN** the caller runtime SHALL be the only path that creates or updates `launch_state` for `e1`

#### Scenario: Execution runtime owns terminal lifecycle publication
- **WHEN** execution `e1` reaches `succeeded` or `failed`
- **THEN** the execution runtime for `e1` SHALL publish that terminal lifecycle in `execution_record`
- **AND** caller runtimes SHALL NOT publish those terminal lifecycle values for `e1`

### Requirement: Cancellation-detached lifecycle SHALL describe runtime detachment, not backend completion
`cancel-detached` SHALL mean that the runtime completed its cancellation responsibilities for that execution, removed current-execution ownership by clearing `active/<cache_key>`, and delegated any remaining backend shutdown handling to the adapter or executor contract. `cancel-detached` SHALL NOT mean that external cleanup has fully completed or that the rooted execution graph is fully cancelled.

#### Scenario: Detached lifecycle permits fresh relaunch
- **WHEN** execution `e1` is marked `cancel-detached`
- **THEN** the runtime SHALL allow a future caller for the same `cache_key` to create a new execution attempt

#### Scenario: Detached lifecycle does not prove backend exit
- **WHEN** execution `e1` is marked `cancel-detached`
- **THEN** callers SHALL NOT infer that all external resources for `e1` have already terminated

### Requirement: Best-effort cancellation traversal MAY stop at terminal intermediates
The runtime SHALL perform cancellation traversal from `spawned_execution_ids` on a best-effort basis. If a descendant execution is reachable only through an already-terminal intermediate runtime that is not reconstructed, the runtime MAY leave that descendant running.

#### Scenario: Terminal intermediate prevents deeper cancellation traversal
- **WHEN** execution `A` spawned `B`, `B` spawned `C`, and `B` is already terminal before `A` is cancelled
- **THEN** the runtime MAY cancel `A` without cancelling `C`
- **AND** that outcome SHALL be treated as an accepted limitation of best-effort cancellation
