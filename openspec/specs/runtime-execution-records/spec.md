## Purpose

Define runtime execution identity, persisted state, coordination, cancellation, and inspection behavior.

## Requirements

### Requirement: Runtime SHALL separate cache identity from execution identity
The runtime SHALL treat `cache_key` as stable computation identity and UUID7 `execution_id` as one attempt's identity. `cache/<cache_key>` SHALL contain only the current execution ID. `execution/<execution_id>` SHALL contain that attempt's state. S3 conditional operations, not UUID ordering, SHALL select the current execution.

#### Scenario: First caller claims a cache key
- **WHEN** a caller observes no cache pointer
- **THEN** it creates a fresh execution record before conditionally creating the pointer

#### Scenario: Later caller joins the current attempt
- **WHEN** `cache/ck1` contains `e1`
- **THEN** a caller reads `execution/e1`
- **AND** it SHALL NOT create another attempt solely because `e1` is running or its lock expired

### Requirement: Runtime SHALL maintain an active execution pointer per cache key
The runtime SHALL persist the current execution for a cache key at `cache/<cache_key>` as plain text containing only the execution ID. The pointer SHALL exist from successful reservation until conditional deletion by cancelation or invalidation, including while the execution is running and after it stores a reusable terminal result.

#### Scenario: Reservation publishes current execution
- **WHEN** execution `e1` wins reservation for cache key `ck1`
- **THEN** `cache/ck1` contains only `e1`

#### Scenario: Terminal result preserves pointer
- **WHEN** execution `e1` stores a terminal result
- **THEN** `cache/ck1` continues to contain `e1`

#### Scenario: Missing execution behind pointer is stale
- **WHEN** `cache/ck1` contains an execution ID whose record is missing
- **THEN** the runtime SHALL conditionally repair or remove the stale pointer before continuing

### Requirement: Runtime SHALL maintain one mutable execution record per execution id
The runtime SHALL represent each execution ID with three exact JSON objects: immutable `execution/<execution_id>/metadata.json`, shared semantic `execution/<execution_id>/state.json`, and caller coordination `execution/<execution_id>/driver.json`. No object SHALL contain a schema-version field.

`metadata.json` SHALL contain exactly `execution_id`, `cache_key`, `argv_ref`, and `created_at`. `execution_id` SHALL be nonempty; `cache_key` SHALL be null or nonempty; `argv_ref` SHALL be a syntactically typed `node-argv` ref string or null; and `created_at` SHALL be a non-boolean nonnegative integer.

`state.json` SHALL contain exactly `lifecycle`, `result_ref`, `result_source`, `spawned_execution_ids`, `child_execution_ids`, `cancelation`, `invalidation`, and `updated_at`. Lifecycle SHALL be one of `pending`, `running`, `succeeded`, `failed`, `cancel-pending`, or `canceled`. `result_ref` SHALL be a syntactically typed `dag` ref string or null and `result_source` SHALL be `runtime`, `adapter-error`, or null. The result fields SHALL either both be null or both be non-null. Lineage lists SHALL contain unique nonempty execution IDs and be disjoint. Each control value SHALL be null or an exact object containing nonempty `requested_by` and non-boolean nonnegative integer `requested_at`.

`driver.json` SHALL contain exactly `lock`, `not_before`, `adapter_state`, and `cleanup`. `lock` SHALL be null or exact `{owner: nonempty str, ttl: positive finite non-boolean number}`. `not_before` SHALL be null or a non-boolean nonnegative integer timestamp. `adapter_state` SHALL be an object or null. `cleanup` SHALL be null or exact `{status: "complete" | "failed", error: str | null}`, with null error for `complete` and a nonempty error for `failed`.

#### Scenario: Fresh execution creates all three objects
- **WHEN** the runtime reserves execution `e1` for cache key `ck1`
- **THEN** it creates immutable metadata, pending semantic state, and unlocked driver state before publishing the cache pointer
- **AND** state has null result fields, empty lineage, and null control state
- **AND** driver has null not-before, adapter, and cleanup fields

#### Scenario: Runtime result publication is independently visible
- **WHEN** the funk runtime for `e1` publishes DAG `dag:d1`
- **THEN** it conditionally stores `result_ref = "dag:d1"` and `result_source = "runtime"` in `state.json`
- **AND** it does not mutate lifecycle or acquire the driver lock

#### Scenario: Adapter failure publishes a failed result
- **WHEN** the coordinating caller commits adapter failure DAG `dag:error`
- **THEN** one guarded state mutation stores lifecycle `failed`, `result_ref = "dag:error"`, and `result_source = "adapter-error"`

#### Scenario: Fresh child record is complete and locked
- **WHEN** the runtime reserves execution `e1` for cache key `ck1`
- **THEN** it creates all three objects and acquires the fresh driver lock

#### Scenario: Result is stored in the same record
- **WHEN** execution `e1` publishes DAG `dag:d1`
- **THEN** `state.json` stores that result in the execution's semantic section

#### Scenario: Every mutation requires ownership
- **WHEN** a caller changes driver-owned fields
- **THEN** it holds the matching driver owner, while state writers use guarded state CAS

#### Scenario: Cancel-pending is the only cancellation intermediate
- **WHEN** semantic state is validated or written
- **THEN** cancel-pending is the only accepted nonterminal cancellation lifecycle

### Requirement: Adapter cancellation SHALL advance directly from cancel-pending to canceled
For every adapter-backed execution from the Phase 1 cancellation set that Phase 2 still observes as `cancel-pending`, Phase 2 SHALL build an `AdapterCancelRequest` from that execution's record, invoke the adapter synchronously while holding the execution lock, and compare-and-swap lifecycle directly to `canceled` only after status `cancelled`. Retry responses SHALL persist adapter state and `driver.not_before`; interrupted, failed, or exhausted cancellation SHALL remain recoverable from `cancel-pending`. Phase 2 SHALL accept `canceled` as concurrent completion and SHALL warn then drop `pending`, `running`, `succeeded`, or `failed` without invoking the cancel adapter.

#### Scenario: Adapter cancellation completes
- **WHEN** the applicable cancel adapter returns `cancelled` for a `cancel-pending` execution
- **THEN** the runtime SHALL compare-and-swap that execution directly to `canceled`

#### Scenario: Cancellation resumes after interruption
- **WHEN** adapter work is interrupted before `canceled` is persisted
- **THEN** the execution SHALL remain `cancel-pending`
- **AND** a later cancellation call SHALL be able to repeat the idempotent cancel operation

#### Scenario: Already canceled selection is complete
- **WHEN** Phase 2 observes a selected execution as `canceled`
- **THEN** it SHALL drop that execution without adapter invocation

#### Scenario: Unexpected selected lifecycle does not reach adapter
- **WHEN** Phase 2 observes a selected execution as `pending`, `running`, `succeeded`, or `failed`
- **THEN** it SHALL warn with the execution ID and lifecycle
- **AND** it SHALL drop that execution without adapter invocation

### Requirement: Runtime SHALL expose descendant execution graphs from execution records
The runtime SHALL expose an execution-record-owned graph query that accepts root execution ids and returns only the reachable descendant closure from those roots. The payload SHALL have shape `{roots: list[str], nodes: dict[str, node_payload]}` where each `node_payload` contains `execution_id`, `cache_key`, `lifecycle`, `updated_at`, `created_at`, `cancelation`, `children`, and `spawned`. `children` SHALL be derived from `child_execution_ids`, and `spawned` SHALL be derived from `spawned_execution_ids`. The graph query SHALL read only execution-record objects and SHALL include each reachable execution at most once.

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
- **THEN** it SHALL NOT require DAG objects, edge files, or cache pointers to shape the response

### Requirement: Runtime SHALL return raw execution records for direct record reads
The runtime SHALL support direct execution inspection by execution ID and SHALL return `{metadata, state, driver}` containing the three stored execution objects without flattening or synthesizing legacy unified-record fields. A missing required object SHALL raise `DmlRepoError`.

#### Scenario: Direct read returns the three stored sections
- **WHEN** all three objects exist for execution `e1`
- **THEN** direct inspection returns exact `metadata`, `state`, and `driver` sections

#### Scenario: Partial execution state is invalid
- **WHEN** one or more required objects for execution `e1` are absent
- **THEN** direct inspection raises `DmlRepoError`

#### Scenario: Direct record read returns the stored execution record unchanged
- **WHEN** all three execution objects for `e1` exist
- **THEN** direct inspection returns those exact stored sections

#### Scenario: Direct record read does not reshape into a graph or summary payload
- **WHEN** a caller reads execution `e1`
- **THEN** the runtime does not synthesize graph or summary fields outside the three schemas

#### Scenario: Direct record read surfaces missing-record failure
- **WHEN** any required object for the requested execution is missing
- **THEN** the runtime raises `DmlRepoError`

### Requirement: Cache refs SHALL remain proper refs and record execution ids
The runtime SHALL retain plain `cache/<cache_key>` execution-ID pointers. A cache reader SHALL resolve `state.json` and materialize its typed `result_ref`. Lifecycles `succeeded` and `failed` SHALL be reusable cache hits only when `result_ref` is present and neither cancelation nor invalidation blocks reuse. Cleanup SHALL be required when a result is present and `driver.cleanup` is null; `complete` and `failed` cleanup records SHALL be terminal and SHALL NOT require another cleanup call. A reusable result SHALL remain returnable while cleanup is pending, retry-delayed, complete, or failed. Before returning either a cached terminal result or a result established by a terminal invoke outcome, the coordinating caller SHALL give required cleanup one adapter call when it owns the driver and `driver.not_before` does not defer the operation. Cleanup retry SHALL persist continuation and timing while leaving cleanup required, and cleanup success or failure SHALL persist its terminal cleanup record. Cleanup retry, deferral, or failure SHALL NOT invalidate or replace the result.

#### Scenario: Successful terminal cache lookup
- **WHEN** `cache/ck1` contains `e1` and state is succeeded with `result_ref = "dag:d1"`
- **THEN** cache lookup materializes `dag:d1`

#### Scenario: Error DAG remains cached
- **WHEN** state is failed with an adapter-error DAG in `result_ref`
- **THEN** cache lookup returns that error DAG

#### Scenario: Cleanup does not block a reusable result
- **WHEN** a terminal execution has a reusable result and required cleanup is retry-delayed
- **THEN** the caller returns the result without calling cleanup before the shared deadline

#### Scenario: Running execution is not a cache result
- **WHEN** a cache pointer names a running execution
- **THEN** cache lookup reports that the result is not ready

#### Scenario: Cached terminal result drives required cleanup
- **WHEN** a caller owns a cached terminal execution with a result, null cleanup, and no active retry delay
- **THEN** it calls cleanup once and persists the cleanup response before returning the result

#### Scenario: Fresh terminal invoke drives required cleanup
- **WHEN** invoke establishes a successful or failed terminal result while its caller owns the driver and cleanup is null
- **THEN** that caller calls cleanup once and persists the cleanup response before returning the result

#### Scenario: Terminal cleanup is not repeated
- **WHEN** a reusable execution records cleanup as complete or failed
- **THEN** the caller returns the result without another cleanup call

#### Scenario: Cleanup retry preserves result delivery
- **WHEN** cleanup returns retry while a caller is preparing to return a reusable result
- **THEN** the caller persists adapter state and the shared retry deadline
- **AND** it returns the unchanged result with cleanup still required

### Requirement: Adapter operations SHALL follow the runtime-owned execution contract
The runtime SHALL use distinct invoke, cleanup, and cancel requests. Repeated invoke requests SHALL carry current `adapter_state` and SHALL be the only start-or-status-check operation. Cleanup requests SHALL require a populated `result_ref` and SHALL carry that ref with current adapter state. Cancel requests SHALL be sent only for `cancel-pending`, carry `argv_ref` from metadata, respect persisted `driver.not_before`, and persist retry continuation state. Adapters SHALL NOT receive or mutate complete metadata, state, or driver objects.

#### Scenario: First adapter call uses null adapter state
- **WHEN** the runtime invokes an adapter for a new execution
- **THEN** the invoke request SHALL include null `adapter_state`

#### Scenario: Repeated invoke uses durable state
- **WHEN** an invoke response previously supplied adapter state
- **THEN** the next invoke for the same execution ID SHALL include that state
- **AND** no separate poll operation SHALL be used

#### Scenario: Cleanup receives the published result
- **WHEN** the caller drives cleanup for execution `e1`
- **THEN** the cleanup request SHALL include `e1`'s non-null result ref and current adapter state

#### Scenario: Cancel update uses execution-owned target
- **WHEN** the runtime invokes cancellation for a `cancel-pending` execution
- **THEN** it SHALL send that execution ID and `metadata.argv_ref`

#### Scenario: Runtime ignores unsuccessful cancel return for terminal lifecycle write
- **WHEN** a cancel adapter returns an outcome other than `cancelled`
- **THEN** runtime-owned coordination SHALL leave lifecycle `cancel-pending`

#### Scenario: Every remaining cancel-pending execution receives its cancel update
- **WHEN** Phase 1 selects a parent and one or more spawned adapter-backed executions
- **THEN** Phase 2 SHALL process the cancel adapter for each execution it still observes as `cancel-pending`

#### Scenario: Lifecycle drift is filtered before adapter request
- **WHEN** Phase 2 observes selected work outside `cancel-pending`
- **THEN** it SHALL apply lifecycle filtering without sending a cancel request

#### Scenario: Cancellation requester is stable across the selected set
- **WHEN** root cancellation selects nested executions
- **THEN** persisted cancellation records SHALL identify and preserve that requester

#### Scenario: Pending response is rejected
- **WHEN** an adapter returns status pending
- **THEN** the runtime SHALL treat it as a failure code requiring diagnostics, not a retry status

### Requirement: Stale lock recovery SHALL preserve active execution ownership
If the current execution's `driver.json.lock` is expired, a caller SHALL attempt to steal that lock by CAS against `driver.json` and resume the same execution ID. It SHALL NOT mutate immutable `metadata.json`, conflate the lock with semantic `state.json`, or create a replacement attempt while the cache pointer still names the existing reusable or resumable execution.

#### Scenario: Expired current execution resumes
- **WHEN** `exec/cache/ck1` contains `e1` and `exec/execution/e1/driver.json.lock` is expired
- **THEN** a caller MAY CAS a new owner into `driver.json.lock`
- **AND** it resumes `e1` without replacing `metadata.json` or `state.json`

### Requirement: Failed execution SHALL be cached as a terminal result
If invoke reports a failure code or malformed terminal output, the coordinating caller SHALL commit an error DAG, atomically store it with `result_source = "adapter-error"` and lifecycle `failed`, and retain the cache pointer. Cleanup failure SHALL NOT create an execution error DAG and SHALL NOT change execution lifecycle.

#### Scenario: Failed invoke populates cache
- **WHEN** invoke reports a failure for a cache key
- **THEN** the caller stores an adapter-error DAG and failed lifecycle in state
- **AND** the current cache pointer remains bound to that execution

#### Scenario: Cleanup failure preserves execution outcome
- **WHEN** cleanup fails after a runtime result has established lifecycle succeeded
- **THEN** driver cleanup becomes failed with diagnostic error text
- **AND** lifecycle and result ref remain unchanged

#### Scenario: Failed adapter result populates cache
- **WHEN** invoke reports a failure code for a cache key
- **THEN** the caller stores an adapter-error DAG and retains the cache pointer

#### Scenario: Failed execution remains reusable
- **WHEN** failed semantic state has a non-null adapter-error result
- **THEN** cache lookup returns its error DAG

### Requirement: Commit lifecycle distinction SHALL be documented in code and spec
The runtime SHALL document at the `IndexOps.commit` lifecycle update site that committing an `Error` value is still a successful execution, and that runtime `failed` is reserved for execution-path failures that prevent successful DAG completion.

#### Scenario: Commit lifecycle distinction is documented at implementation site
- **WHEN** maintainers inspect the execution-record lifecycle update in `IndexOps.commit`
- **THEN** the code includes a comment explaining why committed `Error` values still map to `lifecycle = "succeeded"`
- **AND** the comment distinguishes DAG error results from runtime execution failures

### Requirement: Runtime SHALL separate caller-owned launch state from runtime-owned lifecycle state
The execution files SHALL separate authority by field. A funk runtime SHALL conditionally publish only its normal `result_ref`, matching `result_source`, and runtime lineage summaries in `state.json`. Coordinating callers SHALL own lifecycle transitions, adapter-error result publication, and every `driver.json` mutation. Control workflows SHALL own cancelation and invalidation fields. Adapters and executors SHALL receive projections and SHALL NOT mutate execution files directly.

#### Scenario: Runtime publishes without driver ownership
- **WHEN** an active funk runtime commits its result DAG
- **THEN** it publishes the runtime result through guarded state CAS without holding the driver lock

#### Scenario: Caller persists adapter continuation data
- **WHEN** invoke returns adapter state
- **THEN** the current driver-lock owner conditionally persists it in `driver.json`

#### Scenario: Adapter sees only request projection
- **WHEN** any adapter operation is sent
- **THEN** it contains only operation inputs defined by the adapter protocol
- **AND** it does not expose lock, lifecycle, lineage, cancelation, or invalidation fields

#### Scenario: Runtime persists adapter continuation data
- **WHEN** an adapter call returns continuation state
- **THEN** the current driver owner stores it in `driver.adapter_state`

#### Scenario: Adapter side effects remain external
- **WHEN** an adapter performs execution work
- **THEN** it may use execution-owned IO but does not mutate any execution object

### Requirement: Fresh pre-adapter launch failures SHALL clean owned artifacts
If a fresh execution launch fails before the runtime calls its adapter, the runtime SHALL conditionally remove that launch's caller edge, matching cache pointer, and unchanged execution record. It SHALL not delete reused executions. Once the adapter call has begun, it SHALL retain the execution record because external work may exist.

#### Scenario: Lineage registration fails before adapter call
- **WHEN** a fresh execution's edge or caller lineage registration fails
- **THEN** the runtime removes only that launch's edge, matching pointer, and unchanged record

#### Scenario: Adapter call has begun
- **WHEN** an exception occurs after the adapter call begins
- **THEN** the runtime retains the execution record

### Requirement: Best-effort cancellation traversal MAY stop at terminal intermediates
The runtime SHALL perform cancellation traversal from `spawned_execution_ids` on a best-effort basis. If a descendant execution is reachable only through an already-terminal intermediate runtime that is not reconstructed, the runtime MAY leave that descendant running.

#### Scenario: Terminal intermediate prevents deeper cancellation traversal
- **WHEN** execution `A` spawned `B`, `B` spawned `C`, and `B` is already terminal before `A` is cancelled
- **THEN** the runtime MAY cancel `A` without cancelling `C`
- **AND** that outcome SHALL be treated as an accepted limitation of best-effort cancellation

### Requirement: Runtime SHALL durably register a child before adapter invocation
For an adapter-backed child execution, the runtime SHALL publish the caller edge and append the child execution ID to the caller's `spawned_execution_ids` through successful coordinated updates before invoking the adapter. Caller registration SHALL serialize with cancellation selection for the callee and SHALL verify that the callee lifecycle still permits invocation. The runtime SHALL retry CAS conflicts with bounded backoff. If registration observes `cancel-pending` or `canceled`, or exhausts its retry budget, it SHALL fail the launch, remove any incomplete caller edge it owns, and SHALL NOT invoke the adapter.

#### Scenario: Cancellation selection wins child-registration contention
- **WHEN** cancellation persists `cancel-pending` for callee `e1` before caller registration completes
- **THEN** registration of `e1` SHALL fail
- **AND** the runtime SHALL remove its incomplete caller edge
- **AND** it SHALL NOT invoke `e1`'s adapter

#### Scenario: Child registration wins cancellation contention
- **WHEN** registration completes a valid caller edge for `e1` before cancellation evaluates caller references
- **THEN** cancellation planning SHALL observe that valid caller reference
- **AND** it SHALL leave `e1` active

#### Scenario: Child registration exhausts retries
- **WHEN** registration cannot complete after the bounded CAS retry budget
- **THEN** the runtime SHALL raise a coordination failure
- **AND** it SHALL NOT invoke `e1`'s adapter

### Requirement: Runtime SHALL surface terminal-child bookkeeping exhaustion
The runtime SHALL move a normally terminal direct child from `spawned_execution_ids` to `child_execution_ids` through a compare-and-swap update with bounded backoff. If that update exhausts its retry budget, the runtime SHALL surface a coordination failure and SHALL preserve state needed for a later terminal poll to retry the update.

#### Scenario: Terminal-child bookkeeping exhausts retries
- **WHEN** caller `e0` cannot record terminal child `e1` after the bounded CAS retry budget
- **THEN** the runtime SHALL surface a coordination failure
- **AND** it SHALL not silently report bookkeeping success

### Requirement: Result publication SHALL determine caller lifecycle finalization
A caller observing `result_source = "runtime"` on an active execution SHALL conditionally transition lifecycle to `succeeded`. Adapter-error publication SHALL store lifecycle `failed` in the same state mutation. Cleanup responses SHALL never determine or revise execution lifecycle.

#### Scenario: Caller finalizes a runtime result
- **WHEN** a caller observes an active execution with a runtime result
- **THEN** it conditionally transitions lifecycle to succeeded

#### Scenario: Funk Error result is still successful execution
- **WHEN** the runtime-published DAG contains a user-level Error
- **THEN** `result_source = "runtime"` still leads to lifecycle succeeded

### Requirement: V0 execution storage SHALL have no legacy compatibility
The runtime SHALL read and write only the three-file v0 execution layout. It SHALL NOT parse, migrate, or preserve the former unified execution-record schema. A cache pointer whose execution ID lacks the required three-file layout SHALL be treated as stale.

#### Scenario: Legacy cache pointer is stale
- **WHEN** a cache pointer names an execution represented only by a unified legacy record
- **THEN** the runtime treats the pointer as stale and repairs or removes it

### Requirement: Execution state mutations SHALL obey field authority
Every mutation of `state.json` SHALL use compare-and-swap against the latest object. Mutations that change `spawned_execution_ids`, `child_execution_ids`, or the inseparable `result_ref` and `result_source` pair MAY proceed without the driver lock. Every mutation that changes lifecycle, cancelation, invalidation, or any other non-derived state field SHALL require the current driver lock owner and a lifecycle that permits that operation. Updating `updated_at` as part of an otherwise authorized mutation SHALL use the authority of that mutation. No persisted schema fields or lifecycle values SHALL be added.

#### Scenario: Result publication is lock-free CAS
- **WHEN** a running execution publishes a runtime result
- **THEN** it SHALL atomically CAS-update `result_ref`, `result_source`, and `updated_at` without requiring the driver lock

#### Scenario: Lineage summary mutation is lock-free CAS
- **WHEN** a caller registers or completes a direct child
- **THEN** it SHALL CAS-update the caller's lineage arrays and `updated_at` without requiring the caller's driver lock

#### Scenario: Lifecycle mutation requires lock ownership
- **WHEN** a writer attempts to change execution lifecycle
- **THEN** it SHALL hold the current execution driver lock
- **AND** it SHALL conditionally update the latest `state.json`

#### Scenario: Control mutation requires lock ownership
- **WHEN** a writer attempts to change cancelation or invalidation metadata
- **THEN** it SHALL hold the current execution driver lock
- **AND** it SHALL conditionally update the latest `state.json`

#### Scenario: Cancel-pending rejects unrelated control mutation
- **WHEN** a control writer rereads lifecycle `cancel-pending`
- **THEN** it SHALL not change invalidation or other unrelated state fields
- **AND** only normal terminal-child bookkeeping or cancellation completion MAY change that state

### Requirement: Execution lifecycle transitions SHALL be absorbing and guarded
The runtime SHALL permit only `pending -> running`, `pending -> cancel-pending`, `running -> succeeded`, `running -> failed`, `running -> cancel-pending`, and `cancel-pending -> canceled`. Every lifecycle CAS retry SHALL reread and revalidate its source lifecycle. The lifecycle values `succeeded`, `failed`, and `canceled` SHALL be absorbing and SHALL never transition to another lifecycle.

#### Scenario: Activation requires pending
- **WHEN** activation attempts to mark an execution running
- **THEN** it SHALL succeed only if the latest lifecycle is `pending`

#### Scenario: Normal completion requires running
- **WHEN** a coordinator attempts to mark an execution succeeded or failed
- **THEN** it SHALL succeed only if the latest lifecycle is `running`

#### Scenario: Cancellation completion requires cancel-pending
- **WHEN** a cancellation coordinator attempts to mark an execution canceled
- **THEN** it SHALL succeed only if the latest lifecycle is `cancel-pending`

#### Scenario: Terminal lifecycle is absorbing
- **WHEN** an execution lifecycle is `succeeded`, `failed`, or `canceled`
- **THEN** no writer SHALL change that lifecycle

### Requirement: Lock-free state writers SHALL respect lifecycle
Runtime result publication SHALL modify only a `running` execution. Initial spawned-child registration SHALL modify only a `running` caller. Normally terminal child bookkeeping SHALL atomically remove the child from `spawned_execution_ids` and add it to `child_execution_ids` when the caller is `running` or `cancel-pending`. If bookkeeping observes `cancel-pending`, it SHALL persist the lineage update before surfacing cancellation. Lock-free state mutation SHALL reject `pending`, `succeeded`, `failed`, and `canceled`, except where no field value would change.

#### Scenario: Cancellation blocks result publication
- **WHEN** result publication rereads lifecycle `cancel-pending` or `canceled`
- **THEN** it SHALL reject the publication without changing result fields

#### Scenario: Cancellation blocks initial child registration
- **WHEN** initial child registration rereads a caller lifecycle other than `running`
- **THEN** it SHALL not add the child to `spawned_execution_ids`

#### Scenario: Terminal child completes during caller cancellation
- **WHEN** a normally terminal child remains spawned and its caller is `cancel-pending`
- **THEN** the runtime SHALL CAS-move the child from spawned to completed lineage
- **AND** it SHALL surface cancellation only after that update succeeds

#### Scenario: Terminal state rejects lock-free mutation
- **WHEN** a lock-free writer rereads `succeeded`, `failed`, or `canceled`
- **THEN** it SHALL not change result or lineage state
