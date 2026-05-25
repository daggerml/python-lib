### Requirement: Runtime SHALL separate cache identity from execution identity
The runtime SHALL treat `cache_key` as the stable computation identity and `execution_id` as the stable identity of one execution attempt. The runtime SHALL acquire execution coordination locks by `cache_key` for launch, resume, and cancellation, SHALL propagate `execution_id` in the adapter envelope, and SHALL use execution id as the identity for dependency edges, execution state objects, and invalidation records.

#### Scenario: First launch creates a new execution identity
- **WHEN** `start_fn` observes a cache miss and confirms there is no active execution for the computed `cache_key`
- **THEN** it creates a new `execution_id` for that launch attempt
- **AND** it invokes the adapter with both `cache_key` and `execution_id`

#### Scenario: Resume preserves the current execution identity
- **WHEN** `start_fn` observes an active execution for a `cache_key`
- **THEN** it SHALL reuse the referenced `execution_id`
- **AND** it SHALL NOT create a new `execution_id` for that execution while resuming it

#### Scenario: Cancellation resolves lock identity from the execution record
- **WHEN** cancellation targets execution `e1`
- **AND** `exec/state/e1.json` records `cache_key = "ck1"`
- **THEN** the runtime SHALL acquire the execution coordination lock for `ck1`
- **AND** it SHALL continue to use `e1` as the execution-record and dependency-graph identity

### Requirement: Runtime SHALL maintain an active execution pointer per cache key
The runtime SHALL persist the currently active execution for a `cache_key` at `active/<cache_key>` as plain text containing only the `execution_id`.

#### Scenario: Active pointer is created for a new running execution
- **WHEN** the first adapter call for a new execution returns `running`
- **THEN** the runtime SHALL create `active/<cache_key>` containing that execution's `execution_id`

#### Scenario: Stale active pointer is discarded
- **WHEN** `active/<cache_key>` exists but `exec/state/<execution_id>.json` does not exist
- **THEN** the runtime SHALL delete `active/<cache_key>`
- **AND** it SHALL treat the cache key as having no active execution

### Requirement: Runtime SHALL maintain one mutable execution record per execution id
The runtime SHALL persist one mutable lifecycle object per execution id as `execution_record`, separate from caller-owned `launch_state`. `execution_record` SHALL include `execution_id`, `cache_key`, `lifecycle`, `updated_at`, `spawned_execution_ids`, and `cancellation_requested_by`, where `cancellation_requested_by` is `str | null`. `lifecycle` SHALL be one of `running`, `cancel-pending`, `cancelled`, `succeeded`, or `failed`. `spawned_execution_ids` SHALL be the deduped set of child execution ids started by that execution for cancellation traversal. `execution_record` updates SHALL use compare-and-swap with the latest known ETag. If a compare-and-swap update observes ETag drift, the runtime SHALL reread the record and SHALL raise cancellation interruption only when the reread lifecycle is already a `cancel-*` value; otherwise it SHALL continue from the latest valid reread state.

The same `execution_record` schema SHALL also be used for each live index id. For index-root records, the object path SHALL be `exec/state/<index_id>.json`, `execution_id` SHALL equal the `index_id`, `cache_key` SHALL equal the `index_id`, and `spawned_execution_ids` SHALL track the deduped set of execution ids started from that index.

`IndexOps.commit` SHALL always finalize the committing execution or root record as `lifecycle = "succeeded"`. A committed DAG `Error` value SHALL mean the execution successfully produced a DAG whose terminal result is an error. Runtime `failed` SHALL be reserved for execution-path failures that prevent successful DAG completion.

`IndexOps.commit` SHALL always update the committing execution or root record, and it SHALL publish a cache entry only when the committed DAG is runnable (`argv is not null`). Non-runnable DAG commits SHALL still finalize the execution/root record but SHALL NOT publish cache.

The `execution_record` schema SHALL be:

- `execution_id: str`
- `cache_key: str`
- `lifecycle: "running" | "cancel-pending" | "cancelled" | "succeeded" | "failed"`
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
- **AND** the reread lifecycle is `cancel-pending` or `cancelled`
- **THEN** the runtime SHALL surface cancellation interruption rather than continuing normal execution updates

#### Scenario: Top-level cancellation stores user provenance
- **WHEN** a user cancels index `idx1`
- **THEN** `exec/state/idx1.json` SHALL store `cancellation_requested_by` as that user identity

#### Scenario: Nested cancellation stores execution provenance
- **WHEN** execution `e1` triggers `cancel(e2)` during nested cancellation
- **THEN** `exec/state/e2.json` SHALL store `cancellation_requested_by = "e1"`

#### Scenario: Cancellation terminal state is cancelled
- **WHEN** runtime cancellation completes for execution `e1`
- **THEN** `exec/state/e1.json` SHALL store `lifecycle = "cancelled"`

#### Scenario: Root record accumulates spawned execution ids
- **WHEN** index `idx1` starts execution `e1`
- **THEN** the runtime SHALL update `exec/state/idx1.json` so that `spawned_execution_ids` contains `e1`

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

### Requirement: Cache refs SHALL remain proper refs and record execution ids
The runtime SHALL publish `refs/cache/<cache_key>.json` as a normal cache ref to the current manifest for that cache key, and that ref SHALL also record `execution_id` for the current execution. Readers that materialize cached results SHALL continue resolving the cached manifest through the ref target, and graph planners SHALL read `execution_id` from the same cache ref.

#### Scenario: Successful execution updates cache pointer
- **WHEN** execution `e7` becomes the terminal cached result for cache key `ck1`
- **THEN** the runtime SHALL write `refs/cache/ck1.json` with `execution_id = "e7"`
- **AND** that object SHALL remain a valid cache ref with its manifest `target`

#### Scenario: Runnable DAG publication uses explicit execution identity
- **WHEN** an execution-aware worker commits a runnable DAG result
- **THEN** the runtime publishes the cache entry using the explicit `execution_id` provided through the runtime execution-aware call path
- **AND** it does not discover that identity through a process-local execution context object

#### Scenario: Re-run requires prior invalidation
- **WHEN** a later execution `e8` attempts to publish a terminal cached result for cache key `ck1`
- **AND** `refs/cache/ck1.json` already exists for an earlier execution
- **THEN** the runtime SHALL reject that cache publication
- **AND** the earlier cache ref MUST be invalidated or deleted before `e8` can publish `refs/cache/ck1.json`

### Requirement: Adapter envelope and result schema SHALL follow the runtime-owned execution contract
The adapter envelope SHALL include `argv_ptr`, `cache_key`, `execution_id`, `remote`, `runnable`, `state`, `execution_status`, and `cancel_requested_by`. For cancellation, the runtime SHALL send `execution_status = "cancel-pending"` only to adapter chains responsible for direct child executions.

Cancel-path adapter returns SHALL NOT control runtime lifecycle persistence. The runtime SHALL ignore those cancel return values when deciding whether to write `cancelled`.

#### Scenario: First adapter call uses null state
- **WHEN** the runtime invokes an adapter for a new execution
- **THEN** the adapter envelope SHALL include `state = null`

#### Scenario: Cancel update includes pending lifecycle and requester provenance
- **WHEN** the runtime invokes an adapter for a cancel update
- **THEN** the adapter envelope SHALL include `execution_status = "cancel-pending"`
- **AND** it SHALL include `cancel_requested_by`

#### Scenario: Runtime ignores cancel return for terminal lifecycle write
- **WHEN** an adapter returns from a cancel update
- **THEN** the runtime SHALL NOT require a `cancelled` adapter result before writing `lifecycle = "cancelled"`

#### Scenario: Parent cancellation targets child adapter chains only
- **WHEN** `cancel(e1)` fans out over direct children
- **THEN** the runtime SHALL send cancel updates only to adapter chains responsible for those child executions
- **AND** it SHALL NOT separately send a cancel update for `e1` itself as part of that parent cancel flow

#### Scenario: Immediate parent is recorded for nested cancellation
- **WHEN** root cancellation of `idx1` leads to nested `cancel(e2)` through `e1`
- **THEN** `exec/state/e2.json` SHALL record `cancellation_requested_by = "e1"`
- **AND** it SHALL NOT replace that field with `idx1` solely because `idx1` started the overall cancellation tree

#### Scenario: Pending is rejected
- **WHEN** an adapter returns `pending`
- **THEN** the runtime SHALL reject that result as invalid adapter output

### Requirement: Stale lock recovery SHALL preserve active execution ownership
The runtime SHALL use the lock for `cache_key` only to coordinate mutation of the active execution. If a lock is stale and an active execution record exists, the runtime SHALL recover the lock and resume that execution instead of creating a new one.

#### Scenario: Stale lock with active execution resumes existing execution
- **WHEN** the lock for a `cache_key` is stale and `active/<cache_key>` points to an existing execution record
- **THEN** the runtime SHALL recover the lock
- **AND** it SHALL resume the existing `execution_id`
- **AND** it SHALL NOT launch a duplicate execution

### Requirement: Failed execution SHALL be cached as a terminal result
If an adapter returns `failed`, the runtime SHALL complete the DAG with the error and SHALL publish that failed terminal outcome to cache for the `cache_key`.

#### Scenario: Failed adapter result populates cache
- **WHEN** an adapter returns `failed` for a cache key
- **THEN** the runtime SHALL complete the DAG with the reported error
- **AND** it SHALL publish the failed outcome into cache for that cache key

#### Scenario: Failed execution clears active pointer
- **WHEN** an active execution returns `failed`
- **THEN** the runtime SHALL delete `active/<cache_key>` before surfacing the failure

### Requirement: Commit lifecycle distinction SHALL be documented in code and spec
The runtime SHALL document at the `IndexOps.commit` lifecycle update site that committing an `Error` value is still a successful execution, and that runtime `failed` is reserved for execution-path failures that prevent successful DAG completion.

#### Scenario: Commit lifecycle distinction is documented at implementation site
- **WHEN** maintainers inspect the execution-record lifecycle update in `IndexOps.commit`
- **THEN** the code includes a comment explaining why committed `Error` values still map to `lifecycle = "succeeded"`
- **AND** the comment distinguishes DAG error results from runtime execution failures

### Requirement: Runtime SHALL separate caller-owned launch state from runtime-owned lifecycle state
The runtime SHALL treat `launch_state` as caller-owned state for launch and resume, and `execution_record` as execution-runtime-owned state for lifecycle, spawned execution summaries, and cancellation metadata. The caller runtime MAY transition a callee `execution_record` only to `cancel-pending` or `cancelled` during orphan-triggered cancellation, and SHALL NOT otherwise mutate lifecycle state owned by the callee execution runtime.

#### Scenario: Caller runtime owns launch state updates
- **WHEN** `start_fn` launches or resumes execution `e1`
- **THEN** the caller runtime SHALL be the only path that creates or updates `launch_state` for `e1`

#### Scenario: Execution runtime owns terminal lifecycle publication
- **WHEN** execution `e1` reaches `succeeded` or `failed`
- **THEN** the execution runtime for `e1` SHALL publish that terminal lifecycle in `execution_record`
- **AND** caller runtimes SHALL NOT publish those terminal lifecycle values for `e1`

### Requirement: Best-effort cancellation traversal MAY stop at terminal intermediates
The runtime SHALL perform cancellation traversal from `spawned_execution_ids` on a best-effort basis. If a descendant execution is reachable only through an already-terminal intermediate runtime that is not reconstructed, the runtime MAY leave that descendant running.

#### Scenario: Terminal intermediate prevents deeper cancellation traversal
- **WHEN** execution `A` spawned `B`, `B` spawned `C`, and `B` is already terminal before `A` is cancelled
- **THEN** the runtime MAY cancel `A` without cancelling `C`
- **AND** that outcome SHALL be treated as an accepted limitation of best-effort cancellation
