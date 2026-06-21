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
The runtime SHALL persist one mutable lifecycle object per execution id as `execution_record`, separate from caller-owned `launch_state`. `execution_record` SHALL include `execution_id`, `cache_key`, `lifecycle`, `updated_at`, `created_at`, `spawned_execution_ids`, `child_execution_ids`, and `cancellation_requested_by`, where `cancellation_requested_by` is `str | null`. `lifecycle` SHALL be one of `running`, `cancel-pending`, `cancel-ready`, `canceled`, `succeeded`, or `failed`. `spawned_execution_ids` SHALL be the deduped set of active direct child execution ids still in flight for cancellation traversal. `child_execution_ids` SHALL be the deduped set of completed direct child execution ids retained for lineage monitoring. Direct canceled descendants SHALL NOT be moved into `child_execution_ids`. `execution_record` updates SHALL use compare-and-swap with the latest known ETag. If a compare-and-swap update observes ETag drift, the runtime SHALL reread the record and SHALL raise cancellation interruption only when the reread lifecycle is already a `cancel-*` value; otherwise it SHALL continue from the latest valid reread state.

The same `execution_record` schema SHALL also be used for each live index id. For index-root records, the object path SHALL be `exec/state/<index_id>.json`, `execution_id` SHALL equal the `index_id`, `cache_key` MAY be `null` when the root has no lock-bearing execution cache identity, and both `spawned_execution_ids` and `child_execution_ids` SHALL track the deduped direct execution descendants started from that index according to their active vs terminal state.

`IndexOps.commit` SHALL always finalize the committing execution or root record as `lifecycle = "succeeded"`. A committed DAG `Error` value SHALL mean the execution successfully produced a DAG whose terminal result is an error. Runtime `failed` SHALL be reserved for execution-path failures that prevent successful DAG completion.

`IndexOps.commit` SHALL always update the committing execution or root record, and it SHALL publish a cache entry only when the committed DAG is runnable (`argv is not null`). Non-runnable DAG commits SHALL still finalize the execution/root record but SHALL NOT publish cache.

The `execution_record` schema SHALL be:

- `execution_id: str`
- `cache_key: str | null`
- `lifecycle: "running" | "cancel-pending" | "cancel-ready" | "canceled" | "succeeded" | "failed"`
- `updated_at: int`
- `created_at: int`
- `spawned_execution_ids: list[str]`
- `child_execution_ids: list[str]`
- `cancellation_requested_by: str | null`

#### Scenario: Index creation creates the initial execution record
- **WHEN** `IndexOps.create` initializes a new runtime root
- **THEN** it SHALL create an `execution_record` for that root before execution starts
- **AND** that record SHALL use `execution_id = index_id`
- **AND** that record SHALL initialize `created_at` and `updated_at`
- **AND** that record SHALL initialize `spawned_execution_ids = []` and `child_execution_ids = []`

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

### Requirement: Adapter cancel dispatch SHALL target direct children that are cancel-ready
The runtime SHALL dispatch adapter cancellation only for direct spawned child executions of the current execution, and only after the target child's execution record has reached `lifecycle = "cancel-ready"`. Runtime lifecycle ownership remains outside the adapter response contract.

#### Scenario: Parent waits for child cancel-ready before adapter cancel dispatch
- **WHEN** execution `e0` is driving cancellation for direct child `e1`
- **AND** `exec/state/e1.json` is still `cancel-pending`
- **THEN** `F2(e0)` SHALL NOT invoke adapter cancellation for `e1` yet

#### Scenario: Adapter cancel response does not define execution-record lifecycle names
- **WHEN** adapter cancellation is invoked for execution `e1`
- **THEN** the adapter response contract SHALL remain separate from execution-record-only lifecycle values such as `cancel-ready`

### Requirement: Runtime SHALL expose descendant execution graphs from execution records
The runtime SHALL expose an execution-record-owned graph query that accepts root execution ids and returns only the reachable descendant closure from those roots. The payload SHALL have shape `{roots: list[str], nodes: dict[str, node_payload]}` where each `node_payload` contains `execution_id`, `cache_key`, `lifecycle`, `updated_at`, `created_at`, `cancel_requested_by`, `children`, and `spawned`. `children` SHALL be derived from `child_execution_ids`, and `spawned` SHALL be derived from `spawned_execution_ids`. The graph query SHALL read only execution-record objects and SHALL include each reachable execution at most once.

#### Scenario: Graph query returns active and completed descendants
- **WHEN** root execution `e0` has active descendant `e1` in `spawned_execution_ids` and completed descendant `e2` in `child_execution_ids`
- **THEN** the graph payload for root `e0` SHALL include nodes for `e0`, `e1`, and `e2`
- **AND** node `e0` SHALL report `spawned = ["e1"]` and `children = ["e2"]`

#### Scenario: Graph query traverses through completed intermediates
- **WHEN** root execution `e0` lists completed child `e1` in `child_execution_ids`
- **AND** execution `e1` lists child `e2` in either `spawned_execution_ids` or `child_execution_ids`
- **THEN** the graph payload rooted at `e0` SHALL include `e2`

#### Scenario: Graph query excludes unrelated executions
- **WHEN** execution record `e9` exists but is not reachable from the requested roots through `spawned_execution_ids` or `child_execution_ids`
- **THEN** the graph payload SHALL NOT include node `e9`

#### Scenario: Graph query uses execution records only
- **WHEN** the runtime assembles the descendant graph payload
- **THEN** it SHALL NOT require DAG objects, launch-state objects, edge files, active refs, or cache refs to shape the response

### Requirement: Cache refs SHALL remain proper refs and record execution ids
The runtime SHALL publish `refs/cache/<cache_key>.json` as a typed remote ref to the current DAG for that cache key, and that ref SHALL also record `execution_id` in `metadata`.

Readers that materialize cached results SHALL resolve the DAG through `ref.to`, and graph planners SHALL read `execution_id` from the same cache ref metadata.

#### Scenario: Successful execution updates cache pointer
- **WHEN** execution `e7` becomes the terminal cached result for cache key `ck1`
- **THEN** the runtime writes `refs/cache/ck1.json` with `ref.to = "dag:<oid>"`
- **AND** `metadata.execution_id = "e7"`

#### Scenario: Runnable DAG publication uses explicit execution identity
- **WHEN** an execution-aware worker commits a runnable DAG result
- **THEN** the runtime publishes the cache entry using the explicit `execution_id` provided through the runtime execution-aware call path
- **AND** it does not discover that identity through a process-local execution context object

### Requirement: Active execution refs SHALL point to argv roots
The runtime SHALL publish `refs/active/<cache_key>.json` as a typed remote ref to the `node-argv` root for the currently coordinated execution.

#### Scenario: Active execution stores argv root
- **WHEN** execution `e7` claims active coordination for cache key `ck1`
- **THEN** the runtime writes `refs/active/ck1.json` with `ref.to = "node-argv:<oid>"`
- **AND** `metadata.execution_id = "e7"`

#### Scenario: Terminal result does not change active root type
- **WHEN** execution `e7` later produces a terminal DAG result
- **THEN** the runtime publishes that DAG through `cache` or `transport`
- **AND** it does not overwrite `refs/active/ck1.json` with a `dag` root

### Requirement: Transport refs SHALL point to DAG roots
The runtime SHALL publish `refs/transport/<execution_id>.json` as a typed remote ref to a `dag` root.

#### Scenario: Finished execution publishes transport DAG
- **WHEN** execution `e7` finishes and publishes transport state
- **THEN** `refs/transport/e7.json` contains `ref.to = "dag:<oid>"`
- **AND** it contains integer `created` and object `metadata`

#### Scenario: Re-run requires prior invalidation
- **WHEN** a later execution `e8` attempts to publish a terminal cached result for cache key `ck1`
- **AND** `refs/cache/ck1.json` already exists for an earlier execution
- **THEN** the runtime SHALL reject that cache publication
- **AND** the earlier cache ref MUST be invalidated or deleted before `e8` can publish `refs/cache/ck1.json`

### Requirement: Adapter envelope and result schema SHALL follow the runtime-owned execution contract
The adapter envelope SHALL include `argv_ptr`, `cache_key`, `execution_id`, `remote`, `runnable`, `state`, `execution_status`, and `cancel_requested_by`. For cancellation, the runtime SHALL send `execution_status = "cancel-pending"` only to adapter chains responsible for direct child executions.

Cancel-path adapter returns SHALL NOT control runtime lifecycle persistence. The runtime SHALL ignore those cancel return values when deciding whether to write `canceled`.

#### Scenario: First adapter call uses null state
- **WHEN** the runtime invokes an adapter for a new execution
- **THEN** the adapter envelope SHALL include `state = null`

#### Scenario: Cancel update includes pending lifecycle and requester provenance
- **WHEN** the runtime invokes an adapter for a cancel update
- **THEN** the adapter envelope SHALL include `execution_status = "cancel-pending"`
- **AND** it SHALL include `cancel_requested_by`

#### Scenario: Runtime ignores cancel return for terminal lifecycle write
- **WHEN** an adapter returns from a cancel update
- **THEN** the runtime SHALL NOT require a specific adapter success token before writing `lifecycle = "canceled"`

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
The runtime SHALL treat `launch_state` as caller-owned state for launch and resume, and `execution_record` as execution-runtime-owned state for lifecycle, spawned execution summaries, and cancellation metadata. The caller runtime MAY transition a callee `execution_record` only to `cancel-pending` or `canceled` during orphan-triggered cancellation, and SHALL NOT otherwise mutate lifecycle state owned by the callee execution runtime.

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

### Requirement: Contrib migration SHALL NOT change runtime execution-record implementation
This contrib migration SHALL rely on the existing runtime execution-record and adapter-dispatch implementation. It SHALL NOT modify runtime execution-record storage, `Dml.runtime` behavior, `IndexOps`, `ExecutionState`, adapter-envelope production, cache publication, or public API entrypoints outside contrib.

#### Scenario: Existing runtime creates execution-aware index
- **WHEN** contrib needs a worker DAG for a runtime execution
- **THEN** contrib SHALL call the existing public DAG creation path with `cache_key` and `execution_id`
- **AND** the existing runtime implementation SHALL remain responsible for materializing the active argv and maintaining execution records

#### Scenario: Runtime envelope mismatch is encountered
- **WHEN** contrib adapter code encounters a protocol mismatch during implementation
- **THEN** the mismatch SHALL be resolved inside contrib-owned parsing or normalization code if possible
- **AND** runtime/core files SHALL NOT be modified as part of this change
