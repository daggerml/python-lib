## MODIFIED Requirements

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
The runtime SHALL persist `execution/<execution_id>` with fields `execution_id`, `cache_key`, `lifecycle`, `created_at`, `updated_at`, `lock`, `adapter_state`, `argv_ref`, `result_ref`, `spawned_execution_ids`, `child_execution_ids`, `cancelation`, and `invalidation`. `execution_id` SHALL be nonempty; `cache_key` null or nonempty; timestamps non-boolean nonnegative integers with `updated_at >= created_at`; and `lock` null or exact `{owner: nonempty str, ttl: positive finite non-boolean number}`. `adapter_state` SHALL be an object or null. `argv_ref` and `result_ref` SHALL be syntactically typed `node-argv` and `dag` ref strings respectively, or null; validation SHALL NOT check whether either ref exists in storage. Lineage lists SHALL contain unique nonempty execution IDs and be disjoint. `cancelation` and `invalidation` SHALL each be null or exact objects containing nonempty `requested_by` and a non-boolean nonnegative integer `requested_at`. Lifecycle and lineage semantics SHALL remain execution-owned, and every mutation SHALL require the embedded lock owner and CAS.

#### Scenario: Fresh child record is complete and locked
- **WHEN** the runtime reserves execution `e1` for cache key `ck1`
- **THEN** it creates `execution/e1` with lifecycle `pending`, a fresh owner lock, its argv ref, null adapter and result state, empty lineage, and null control state

#### Scenario: Result is stored in the same record
- **WHEN** execution `e1` completes with DAG `dag:d1`
- **THEN** its lock owner conditionally stores `result_ref = "dag:d1"` and a terminal lifecycle in `execution/e1`

#### Scenario: Every mutation requires ownership
- **WHEN** a caller attempts to change lifecycle, adapter state, refs, lineage, cancelation, or invalidation
- **THEN** it SHALL hold the matching execution lock owner

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

### Requirement: Stale lock recovery SHALL preserve active execution ownership
If the current execution lock is expired, a caller SHALL attempt to steal that execution's embedded lock by CAS and resume the same execution ID. It SHALL NOT create a replacement attempt while the cache pointer still names the existing reusable or resumable execution.

#### Scenario: Expired current execution resumes
- **WHEN** `cache/ck1` contains `e1` and `e1` has an expired lock
- **THEN** a caller MAY CAS a new owner into `execution/e1`
- **AND** it resumes `e1`

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

## REMOVED Requirements

### Requirement: Active execution refs SHALL point to argv roots
**Reason**: `argv_ref` now resides in the unified execution record and the cache pointer contains only the execution ID.
**Migration**: None; the v0 layout is intentionally incompatible.

### Requirement: Transport refs SHALL point to DAG roots
**Reason**: `result_ref` now resides in the unified execution record.
**Migration**: None; the v0 layout is intentionally incompatible.

### Requirement: Runtime SHALL expose persisted executor resume state for direct reads
**Reason**: Resume state is replaced by `adapter_state` in the unified execution record.
**Migration**: Callers read adapter state through execution-record inspection; no compatibility alias is provided.
