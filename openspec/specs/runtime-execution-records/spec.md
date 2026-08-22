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
The runtime SHALL persist `execution/<execution_id>` with fields `execution_id`, `cache_key`, `lifecycle`, `created_at`, `updated_at`, `lock`, `adapter_state`, `argv_ref`, `result_ref`, `spawned_execution_ids`, `child_execution_ids`, `cancelation`, and `invalidation`. `execution_id` SHALL be nonempty; `cache_key` null or nonempty; timestamps non-boolean nonnegative integers with `updated_at >= created_at`; and `lock` null or exact `{owner: nonempty str, ttl: positive finite non-boolean number}`. `adapter_state` SHALL be an object or null. `argv_ref` and `result_ref` SHALL be syntactically typed `node-argv` and `dag` ref strings respectively, or null; validation SHALL NOT check whether either ref exists in storage. Lineage lists SHALL contain unique nonempty execution IDs and be disjoint. `cancelation` and `invalidation` SHALL each be null or exact objects containing nonempty `requested_by` and a non-boolean nonnegative integer `requested_at`. Lifecycle SHALL be one of `pending`, `running`, `succeeded`, `failed`, `cancel-pending`, or `canceled`; `cancel-requested` and `cancel-ready` SHALL NOT be accepted. Lifecycle and lineage semantics SHALL remain execution-owned, and every mutation SHALL require the embedded lock owner and CAS.

#### Scenario: Fresh child record is complete and locked
- **WHEN** the runtime reserves execution `e1` for cache key `ck1`
- **THEN** it creates `execution/e1` with lifecycle `pending`, a fresh owner lock, its argv ref, null adapter and result state, empty lineage, and null control state

#### Scenario: Result is stored in the same record
- **WHEN** execution `e1` completes with DAG `dag:d1`
- **THEN** its lock owner conditionally stores `result_ref = "dag:d1"` and a terminal lifecycle in `execution/e1`

#### Scenario: Every mutation requires ownership
- **WHEN** a caller attempts to change lifecycle, adapter state, refs, lineage, cancelation, or invalidation
- **THEN** it SHALL hold the matching execution lock owner

#### Scenario: Cancel-pending is the only cancellation intermediate
- **WHEN** an execution record is validated or written
- **THEN** `cancel-pending` SHALL be accepted as the only nonterminal cancellation lifecycle
- **AND** `cancel-requested` and `cancel-ready` SHALL be rejected

### Requirement: Adapter cancellation SHALL advance directly from cancel-pending to canceled
For every adapter-backed execution in the Phase 1 cancellation set, Phase 2 SHALL build an `AdapterCancelRequest` from that execution's record, invoke the adapter synchronously, and compare-and-swap lifecycle from `cancel-pending` directly to `canceled`. If adapter invocation or lifecycle persistence is interrupted, the execution SHALL remain recoverable from `cancel-pending`, and repeated cancellation SHALL be safe.

#### Scenario: Adapter cancellation completes
- **WHEN** the applicable cancel adapter returns for a `cancel-pending` execution
- **THEN** the runtime SHALL compare-and-swap that execution directly to `canceled`

#### Scenario: Cancellation resumes after interruption
- **WHEN** adapter work is interrupted before `canceled` is persisted
- **THEN** the execution SHALL remain `cancel-pending`
- **AND** a later drive SHALL be able to repeat the idempotent cancel operation

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
The runtime SHALL support a direct single-record read workflow for execution records addressed by execution id. When a caller-facing runtime inspection method requests one execution record, the execution-state layer SHALL read only `execution/<execution_id>` for that id and SHALL return the stored execution record unchanged.

#### Scenario: Direct record read returns the stored execution record unchanged
- **WHEN** `execution/e1` exists
- **THEN** the runtime SHALL return the stored execution record for `e1`
- **AND** the returned payload SHALL preserve every unified execution-record field exactly as stored

#### Scenario: Direct record read does not reshape into a graph or summary payload
- **WHEN** a caller reads execution record `e1`
- **THEN** the runtime SHALL NOT synthesize `children`, `spawned`, or any other derived inspection fields outside the stored execution-record schema

#### Scenario: Direct record read surfaces missing-record failure
- **WHEN** a caller reads execution record `missing`
- **AND** `execution/missing` does not exist
- **THEN** the runtime SHALL raise `DmlRepoError`

### Requirement: Cache refs SHALL remain proper refs and record execution ids
The runtime SHALL replace typed terminal cache refs with plain `cache/<cache_key>` execution-ID pointers. A cache reader SHALL resolve the execution record and materialize its typed `result_ref`. Lifecycles `succeeded` and `failed` SHALL be reusable cache hits only when `result_ref` is present and neither cancelation nor invalidation blocks reuse.

#### Scenario: Successful terminal cache lookup
- **WHEN** `cache/ck1` contains `e1` and `execution/e1` is succeeded with `result_ref = "dag:d1"`
- **THEN** cache lookup materializes `dag:d1`

#### Scenario: Error DAG remains cached
- **WHEN** `execution/e1` is failed with an error DAG in `result_ref`
- **THEN** cache lookup returns that error DAG

#### Scenario: Running execution is not a cache result
- **WHEN** `cache/ck1` contains a running execution
- **THEN** cache lookup reports that the result is not ready

### Requirement: Adapter operations SHALL follow the runtime-owned execution contract
The runtime SHALL use `AdapterInvokeRequest` / `AdapterInvokeResponse` for invocation and `AdapterCancelRequest` / `AdapterCancelResponse` for cancellation. Invoke requests SHALL carry invocation data and current `adapter_state` without cancellation-only fields. Cancel requests SHALL carry `argv_ref` from the unified execution record. Cancel-path adapter responses SHALL NOT control runtime lifecycle persistence. After Phase 1 has selected the complete cancellation set, Phase 2 SHALL issue the applicable cancel operation for each selected adapter-backed execution itself rather than waiting for a child-readiness lifecycle.

#### Scenario: First adapter call uses null adapter state
- **WHEN** the runtime invokes an adapter for a new execution
- **THEN** the `AdapterInvokeRequest` SHALL include null `adapter_state`

#### Scenario: Cancel update uses execution-owned target
- **WHEN** the runtime invokes an adapter for a selected cancellation
- **THEN** the runtime SHALL send an `AdapterCancelRequest` with the target execution ID
- **AND** it SHALL include `argv_ref` from that execution's record

#### Scenario: Runtime ignores cancel return for terminal lifecycle write
- **WHEN** an adapter returns from a cancel update
- **THEN** the runtime SHALL NOT require a specific adapter success token before writing `lifecycle = "canceled"`

#### Scenario: Every selected adapter-backed execution receives its own cancel update
- **WHEN** Phase 1 selects a parent and one or more spawned adapter-backed executions
- **THEN** Phase 2 SHALL process each selected execution's applicable cancel adapter
- **AND** it SHALL NOT require recursive adapter cancellation to discover the selected set

#### Scenario: Cancellation requester is stable across the selected set
- **WHEN** root cancellation selects nested executions
- **THEN** each newly selected execution's `cancelation.requested_by` SHALL identify the requester of that cancellation operation
- **AND** a resumed drive SHALL preserve already-persisted requester metadata

#### Scenario: Pending is rejected
- **WHEN** an adapter returns `pending`
- **THEN** the runtime SHALL reject that result as invalid adapter output

### Requirement: Stale lock recovery SHALL preserve active execution ownership
If the current execution lock is expired, a caller SHALL attempt to steal that execution's embedded lock by CAS and resume the same execution ID. It SHALL NOT create a replacement attempt while the cache pointer still names the existing reusable or resumable execution.

#### Scenario: Expired current execution resumes
- **WHEN** `cache/ck1` contains `e1` and `e1` has an expired lock
- **THEN** a caller MAY CAS a new owner into `execution/e1`
- **AND** it resumes `e1`

### Requirement: Failed execution SHALL be cached as a terminal result
If an adapter reports a non-success outcome, the runtime SHALL commit an error DAG, store it in the current execution's `result_ref`, set lifecycle `failed`, and retain the cache pointer.

#### Scenario: Failed adapter result populates cache
- **WHEN** an adapter reports a non-success outcome for a cache key
- **THEN** the runtime SHALL commit and store an error DAG in `result_ref`
- **AND** the current cache pointer SHALL remain bound to that execution

#### Scenario: Failed execution remains reusable
- **WHEN** a failed execution has a non-null `result_ref`
- **THEN** cache lookup returns its error DAG

### Requirement: Commit lifecycle distinction SHALL be documented in code and spec
The runtime SHALL document at the `IndexOps.commit` lifecycle update site that committing an `Error` value is still a successful execution, and that runtime `failed` is reserved for execution-path failures that prevent successful DAG completion.

#### Scenario: Commit lifecycle distinction is documented at implementation site
- **WHEN** maintainers inspect the execution-record lifecycle update in `IndexOps.commit`
- **THEN** the code includes a comment explaining why committed `Error` values still map to `lifecycle = "succeeded"`
- **AND** the comment distinguishes DAG error results from runtime execution failures

### Requirement: Runtime SHALL separate caller-owned launch state from runtime-owned lifecycle state
The unified execution record SHALL store adapter-owned continuation data in `adapter_state` while the runtime owns all persisted fields. Only the current lock owner SHALL update `adapter_state`, lifecycle, refs, lineage summaries, cancelation, or invalidation. Adapters and executors SHALL NOT directly mutate the execution record.

#### Scenario: Runtime persists adapter continuation data
- **WHEN** an adapter call returns state
- **THEN** the current lock owner stores it as `adapter_state` in the execution record

#### Scenario: Adapter side effects remain external
- **WHEN** an adapter performs execution work
- **THEN** it MAY use `io/<execution_id>/`
- **AND** it SHALL NOT mutate `execution/<execution_id>`

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
