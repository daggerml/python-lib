## MODIFIED Requirements

### Requirement: Runtime SHALL maintain one mutable execution record per execution id
The runtime SHALL persist one mutable lifecycle object per execution id as `execution_record`, separate from caller-owned `launch_state`. `execution_record` SHALL include `execution_id`, `cache_key`, `lifecycle`, `updated_at`, `created_at`, `spawned_execution_ids`, `child_execution_ids`, and `cancellation_requested_by`, where `cancellation_requested_by` is `str | null`. `lifecycle` SHALL be one of `pending`, `running`, `cancel-pending`, `cancel-ready`, `canceled`, `succeeded`, or `failed`. `spawned_execution_ids` SHALL be the deduped set of active direct child execution ids still in flight for cancellation traversal. `child_execution_ids` SHALL be the deduped set of completed direct child execution ids retained for lineage monitoring. Direct canceled descendants SHALL NOT be moved into `child_execution_ids`. `execution_record` updates SHALL use compare-and-swap with the latest known ETag. If a compare-and-swap update observes ETag drift, the runtime SHALL reread the record and SHALL raise cancellation interruption only when the reread lifecycle is already a `cancel-*` value; otherwise it SHALL continue from the latest valid reread state.

The same `execution_record` schema SHALL also be used for each live index id. For index-root records, the object path SHALL be `exec/state/<index_id>.json`, `execution_id` SHALL equal the `index_id`, `cache_key` MAY be `null` when the root has no lock-bearing execution cache identity, and both `spawned_execution_ids` and `child_execution_ids` SHALL track the deduped direct execution descendants started from that index according to their active vs terminal state.

`IndexOps.commit` SHALL always finalize the committing execution or root record as `lifecycle = "succeeded"`. A committed DAG `Error` value SHALL mean the execution successfully produced a DAG whose terminal result is an error. Runtime `failed` SHALL be reserved for execution-path failures that prevent successful DAG completion.

`IndexOps.commit` SHALL always update the committing execution or root record, and it SHALL publish a cache entry only when the committed DAG is runnable (`argv is not null`). Non-runnable DAG commits SHALL still finalize the execution/root record but SHALL NOT publish cache.

Execution lifecycle gating for activation and mutation SHALL surface `BadExecutionStatusError` for non-cancel wrong-status failures and `CanceledExecutionError` for cancel-family failures, where `CanceledExecutionError` is a subclass of `BadExecutionStatusError`.

The `execution_record` schema SHALL be:

- `execution_id: str`
- `cache_key: str | null`
- `lifecycle: "pending" | "running" | "cancel-pending" | "cancel-ready" | "canceled" | "succeeded" | "failed"`
- `updated_at: int`
- `created_at: int`
- `spawned_execution_ids: list[str]`
- `child_execution_ids: list[str]`
- `cancellation_requested_by: str | null`

#### Scenario: Index creation creates the initial root execution record
- **WHEN** `IndexOps.create` initializes a new non-execution-aware runtime root
- **THEN** it SHALL create an `execution_record` for that root before execution starts
- **AND** that record SHALL use `execution_id = index_id`
- **AND** that record SHALL initialize `lifecycle = "running"`
- **AND** that record SHALL initialize `created_at` and `updated_at`
- **AND** that record SHALL initialize `spawned_execution_ids = []` and `child_execution_ids = []`

#### Scenario: Child execution reservation creates a pending execution record
- **WHEN** `start_fn` reserves a fresh adapter-backed child execution for cache key `ck1`
- **THEN** it SHALL create `exec/state/<execution_id>.json` before treating that execution as active for `ck1`
- **AND** that record SHALL store `cache_key = "ck1"`
- **AND** that record SHALL initialize `lifecycle = "pending"`

#### Scenario: Execution-aware activation transitions pending to running
- **WHEN** worker bootstrap calls `IndexOps.create(cache_key="ck1", execution_id="e1")`
- **AND** `exec/state/e1.json` exists with `lifecycle = "pending"`
- **THEN** the runtime SHALL create the local mutable index for `e1`
- **AND** it SHALL update `exec/state/e1.json` to `lifecycle = "running"`

#### Scenario: Execution-aware activation rejects already-active or terminal records with wrong-status error
- **WHEN** `IndexOps.create(cache_key="ck1", execution_id="e1")` reads `exec/state/e1.json`
- **AND** the lifecycle is `running`, `succeeded`, or `failed`
- **THEN** it SHALL raise `BadExecutionStatusError`
- **AND** it SHALL NOT create or mutate local index state

#### Scenario: Execution-aware activation rejects missing reservation state
- **WHEN** `IndexOps.create(cache_key="ck1", execution_id="e1")` does not find `exec/state/e1.json`
- **THEN** it SHALL raise `DmlRepoError`
- **AND** it SHALL NOT create or mutate local index state

#### Scenario: Execution-aware activation drives cancel-pending before raising canceled error
- **WHEN** `IndexOps.create(cache_key="ck1", execution_id="e1")` reads `exec/state/e1.json`
- **AND** the lifecycle is `cancel-pending`
- **THEN** it SHALL call `ExecutionState.cancel("e1", None, db, mode="drive")`
- **AND** it SHALL raise `CanceledExecutionError`
- **AND** it SHALL NOT create or mutate local index state

#### Scenario: Execution-aware activation rejects terminal cancel states with canceled error
- **WHEN** `IndexOps.create(cache_key="ck1", execution_id="e1")` reads `exec/state/e1.json`
- **AND** the lifecycle is `cancel-ready` or `canceled`
- **THEN** it SHALL raise `CanceledExecutionError`
- **AND** it SHALL NOT create or mutate local index state

#### Scenario: Mutation workflows allow only running execution records
- **WHEN** a mutating workflow other than activation checks `exec/state/e1.json`
- **AND** the lifecycle is `running`
- **THEN** the workflow MAY continue normal mutation

#### Scenario: Mutation workflows reject non-running non-cancel states with wrong-status error
- **WHEN** a mutating workflow other than activation checks `exec/state/e1.json`
- **AND** the lifecycle is `pending`, `succeeded`, or `failed`
- **THEN** it SHALL raise `BadExecutionStatusError`

#### Scenario: Mutation workflows drive cancel-pending before raising canceled error
- **WHEN** a mutating workflow other than activation checks `exec/state/e1.json`
- **AND** the lifecycle is `cancel-pending`
- **THEN** it SHALL call `ExecutionState.cancel("e1", None, db, mode="drive")`
- **AND** it SHALL raise `CanceledExecutionError`

#### Scenario: Mutation workflows reject terminal cancel states with canceled error
- **WHEN** a mutating workflow other than activation checks `exec/state/e1.json`
- **AND** the lifecycle is `cancel-ready` or `canceled`
- **THEN** it SHALL raise `CanceledExecutionError`

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
- **AND** the reread lifecycle is `cancel-pending`, `cancel-ready`, or `canceled`
- **THEN** the runtime SHALL surface cancellation interruption rather than continuing normal execution updates

#### Scenario: Top-level cancellation stores user provenance
- **WHEN** a user cancels index `idx1`
- **THEN** `exec/state/idx1.json` SHALL store `cancellation_requested_by` as that user identity

#### Scenario: Nested cancellation stores execution provenance
- **WHEN** execution `e1` triggers `cancel(e2)` during nested cancellation
- **THEN** `exec/state/e2.json` SHALL store `cancellation_requested_by = "e1"`

#### Scenario: Cancel-ready is a runtime-owned intermediate lifecycle
- **WHEN** runtime cancellation has finished driving direct spawned descendants for execution `e1`
- **THEN** the runtime SHALL persist `exec/state/e1.json` with `lifecycle = "cancel-ready"` before any parent adapter cancel step treats `e1` as ready

#### Scenario: Full cancel writes terminal canceled after F2
- **WHEN** runtime cancellation completes for execution `e1`
- **THEN** `exec/state/e1.json` SHALL store `lifecycle = "canceled"`

#### Scenario: Root record accumulates active spawned execution ids
- **WHEN** index `idx1` starts execution `e1`
- **THEN** the runtime SHALL update `exec/state/idx1.json` so that `spawned_execution_ids` contains `e1`
- **AND** `child_execution_ids` SHALL remain unchanged while `e1` is still active

#### Scenario: Commit of DAG error still records execution success
- **WHEN** an execution successfully commits a DAG whose terminal result is an `Error` value
- **THEN** the runtime SHALL record the committing execution record as `lifecycle = "succeeded"`
- **AND** it SHALL NOT treat that outcome as runtime `failed`

#### Scenario: Runtime failed is reserved for execution-path failure
- **WHEN** an adapter or execution path fails before a DAG result is successfully committed
- **THEN** the runtime MAY record the execution as `lifecycle = "failed"`
- **AND** that lifecycle SHALL describe execution failure rather than a committed DAG error result

#### Scenario: Non-runnable commit finalizes execution without cache publication
- **WHEN** `IndexOps.commit` finalizes an execution or root whose committed DAG has no `argv`
- **THEN** the runtime SHALL still update the committing execution or root record to `lifecycle = "succeeded"`
- **AND** it SHALL NOT publish a cache entry for that commit

#### Scenario: Caller record accumulates spawned execution ids by caller execution id
- **WHEN** caller execution `e0` starts callee execution `e1`
- **THEN** the runtime SHALL read and compare-and-swap update `exec/state/e0.json`
- **AND** the updated `spawned_execution_ids` SHALL contain `e1`
- **AND** that update SHALL not require the caller cache key to be threaded separately

#### Scenario: Root caller uses index execution record
- **WHEN** top-level runtime root `idx1` starts callee execution `e1`
- **THEN** the runtime SHALL treat `idx1` as `caller_execution_id`
- **AND** it SHALL read and compare-and-swap update `exec/state/idx1.json`
- **AND** the updated `spawned_execution_ids` SHALL contain `e1`

#### Scenario: Terminal direct child moves from spawned to child lineage
- **WHEN** caller execution `e0` has active direct child `e1` in `spawned_execution_ids`
- **AND** `e1` reaches terminal lifecycle `succeeded` or `failed`
- **THEN** the runtime SHALL remove `e1` from `e0`'s `spawned_execution_ids`
- **AND** it SHALL add `e1` to `e0`'s `child_execution_ids`

#### Scenario: Canceled direct child does not move into child lineage
- **WHEN** caller execution `e0` has direct child `e1` in `spawned_execution_ids`
- **AND** `e1` reaches lifecycle `canceled`
- **THEN** the runtime SHALL remove `e1` from `e0`'s active cancellation planning as needed
- **BUT** it SHALL NOT add `e1` to `e0`'s `child_execution_ids`

#### Scenario: Spawned and child execution summaries remain disjoint
- **WHEN** the runtime persists an `execution_record`
- **THEN** no execution id SHALL appear in both `spawned_execution_ids` and `child_execution_ids` in the same record
