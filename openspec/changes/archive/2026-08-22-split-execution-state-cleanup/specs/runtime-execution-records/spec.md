## MODIFIED Requirements

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
The runtime SHALL retain plain `cache/<cache_key>` execution-ID pointers. A cache reader SHALL resolve `state.json` and materialize its typed `result_ref`. Lifecycles `succeeded` and `failed` SHALL be reusable cache hits only when `result_ref` is present and neither cancelation nor invalidation blocks reuse. A reusable result SHALL remain returnable while cleanup is pending, retry-delayed, complete, or failed, but each cache-backed invocation SHALL allow an eligible driver to advance pending cleanup before returning.

#### Scenario: Successful terminal cache lookup
- **WHEN** `cache/ck1` contains `e1` and state is succeeded with `result_ref = "dag:d1"`
- **THEN** cache lookup materializes `dag:d1`

#### Scenario: Error DAG remains cached
- **WHEN** state is failed with an adapter-error DAG in `result_ref`
- **THEN** cache lookup returns that error DAG

#### Scenario: Cleanup does not block a reusable result
- **WHEN** a terminal execution has a reusable result and null cleanup state
- **THEN** the caller may return the result after attempting or deferring cleanup according to driver coordination

#### Scenario: Running execution is not a cache result
- **WHEN** a cache pointer names a running execution
- **THEN** cache lookup reports that the result is not ready

### Requirement: Adapter operations SHALL follow the runtime-owned execution contract
The runtime SHALL use distinct invoke, cleanup, and cancel requests. Repeated invoke requests SHALL carry current `adapter_state` and SHALL be the only start-or-status-check operation. Cleanup requests SHALL require a populated `result_ref` and SHALL carry that ref with current adapter state. Cancel requests SHALL carry `argv_ref` from metadata. Adapters SHALL NOT receive or mutate complete metadata, state, or driver objects.

#### Scenario: First adapter call uses null adapter state
- **WHEN** the runtime invokes an adapter for a new execution
- **THEN** the invoke request includes null `adapter_state`

#### Scenario: Repeated invoke uses durable state
- **WHEN** an invoke response previously supplied adapter state
- **THEN** the next invoke for the same execution ID includes that state
- **AND** no separate poll operation is used

#### Scenario: Cleanup receives the published result
- **WHEN** the caller drives cleanup for execution `e1`
- **THEN** the cleanup request includes `e1`'s non-null result ref and current adapter state

#### Scenario: Cancel update uses execution-owned target
- **WHEN** the runtime invokes cancellation for a selected execution
- **THEN** it sends that execution ID and `metadata.argv_ref`

#### Scenario: Runtime ignores cancel return for terminal lifecycle write
- **WHEN** a cancel adapter returns a well-formed response
- **THEN** runtime-owned coordination determines the transition to canceled

#### Scenario: Every selected adapter-backed execution receives its own cancel update
- **WHEN** Phase 1 selects a parent and spawned adapter-backed executions
- **THEN** Phase 2 processes each selected execution's cancel adapter

#### Scenario: Cancellation requester is stable across the selected set
- **WHEN** root cancellation selects nested executions
- **THEN** persisted cancellation records identify and preserve that requester

#### Scenario: Pending is rejected
- **WHEN** an adapter returns status pending
- **THEN** the runtime treats it as a failure code requiring diagnostics, not a retry status

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
- **THEN** cache lookup returns that error DAG

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

## ADDED Requirements

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
