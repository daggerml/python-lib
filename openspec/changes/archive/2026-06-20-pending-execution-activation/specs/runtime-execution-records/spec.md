## MODIFIED Requirements

### Requirement: Runtime SHALL maintain one mutable execution record per execution id
The runtime SHALL persist one mutable lifecycle object per execution id as `execution_record`, separate from caller-owned `launch_state`. `execution_record` SHALL include `execution_id`, `cache_key`, `lifecycle`, `updated_at`, `created_at`, `spawned_execution_ids`, `child_execution_ids`, and `cancellation_requested_by`, where `cancellation_requested_by` is `str | null`. `lifecycle` SHALL be one of `pending`, `running`, `cancel-pending`, `cancel-ready`, `canceled`, `succeeded`, or `failed`. `spawned_execution_ids` SHALL be the deduped set of active direct child execution ids still in flight for cancellation traversal. `child_execution_ids` SHALL be the deduped set of completed direct child execution ids retained for lineage monitoring. Direct canceled descendants SHALL NOT be moved into `child_execution_ids`. `execution_record` updates SHALL use compare-and-swap with the latest known ETag. If a compare-and-swap update observes ETag drift, the runtime SHALL reread the record and SHALL raise cancellation interruption only when the reread lifecycle is already a `cancel-*` value; otherwise it SHALL continue from the latest valid reread state.

The same `execution_record` schema SHALL also be used for each live index id. For index-root records, the object path SHALL be `exec/state/<index_id>.json`, `execution_id` SHALL equal the `index_id`, `cache_key` MAY be `null` when the root has no lock-bearing execution cache identity, and both `spawned_execution_ids` and `child_execution_ids` SHALL track the deduped direct execution descendants started from that index according to their active vs terminal state.

`IndexOps.commit` SHALL always finalize the committing execution or root record as `lifecycle = "succeeded"`. A committed DAG `Error` value SHALL mean the execution successfully produced a DAG whose terminal result is an error. Runtime `failed` SHALL be reserved for execution-path failures that prevent successful DAG completion.

`IndexOps.commit` SHALL always update the committing execution or root record, and it SHALL publish a cache entry only when the committed DAG is runnable (`argv is not null`). Non-runnable DAG commits SHALL still finalize the execution/root record but SHALL NOT publish cache.

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

#### Scenario: Execution-aware activation rejects already-active or terminal records
- **WHEN** `IndexOps.create(cache_key="ck1", execution_id="e1")` reads `exec/state/e1.json`
- **AND** the lifecycle is `running`, `succeeded`, or `failed`
- **THEN** it SHALL raise `DmlRepoError`
- **AND** it SHALL NOT create or mutate local index state

#### Scenario: Execution-aware activation rejects missing reservation state
- **WHEN** `IndexOps.create(cache_key="ck1", execution_id="e1")` does not find `exec/state/e1.json`
- **THEN** it SHALL raise `DmlRepoError`
- **AND** it SHALL NOT create or mutate local index state
