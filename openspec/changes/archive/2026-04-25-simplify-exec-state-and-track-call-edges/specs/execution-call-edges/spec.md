## ADDED Requirements

### Requirement: Runtime SHALL distinguish user-dags from fn-dags for call-edge tracking
For lineage tracking, a user-dag SHALL mean a DAG without an `argv` node and therefore without a cache key. An fn-dag SHALL mean a DAG with an `argv` node and therefore with a cache key.

#### Scenario: User-dag caller has no cache key
- **WHEN** the runtime records a call edge from a DAG that does not have an `argv` node
- **THEN** it SHALL treat that caller as a user-dag identified by `index_id`
- **AND** it SHALL NOT require a caller cache key for that edge

#### Scenario: Fn-dag caller uses cache key identity
- **WHEN** the runtime records a call edge from a DAG that has an `argv` node
- **THEN** it SHALL treat that caller as an fn-dag identified by its caller cache key

### Requirement: Runtime SHALL persist forward call-edge indexes by caller type
The runtime SHALL persist sorted, deduped forward indexes for attempted calls at:

- `calls/from/index/<index_id>.json` containing a list of callee cache keys for user-dag callers
- `calls/from/cache/<caller_ck>.json` containing a list of callee cache keys for fn-dag callers

#### Scenario: User-dag forward lineage is recorded
- **WHEN** a user-dag initiates a new execution for callee cache key `ck1`
- **THEN** the runtime SHALL add `ck1` to `calls/from/index/<index_id>.json`

#### Scenario: Fn-dag forward lineage is recorded
- **WHEN** an fn-dag with caller cache key `ck0` initiates a new execution for callee cache key `ck1`
- **THEN** the runtime SHALL add `ck1` to `calls/from/cache/ck0.json`

### Requirement: Runtime SHALL persist reverse call-edge indexes for callee cache keys
For each callee cache key, the runtime SHALL persist `calls/to/cache/<callee_ck>.json` as an object with two sorted, deduped lists:

- `indexes`: calling user-dag index ids
- `cache_keys`: calling fn-dag cache keys

#### Scenario: Reverse lineage stores user-dag caller
- **WHEN** a user-dag with `index_id` initiates a new execution for callee cache key `ck1`
- **THEN** the runtime SHALL add that `index_id` to `calls/to/cache/ck1.json.indexes`

#### Scenario: Reverse lineage stores fn-dag caller
- **WHEN** an fn-dag with caller cache key `ck0` initiates a new execution for callee cache key `ck1`
- **THEN** the runtime SHALL add `ck0` to `calls/to/cache/ck1.json.cache_keys`

### Requirement: Call-edge indexes SHALL represent attempted invocation lineage
The runtime SHALL write call-edge indexes only on the new-execution path, after it acquires the `cache_key` lock and confirms there is no active execution for that callee cache key. The runtime SHALL NOT delete call-edge indexes on terminal success or failure.

#### Scenario: Resume does not create duplicate lineage writes
- **WHEN** `start_fn` resumes an already active execution for a callee cache key
- **THEN** it SHALL NOT rewrite the forward or reverse call-edge indexes for that execution

#### Scenario: Failed execution preserves lineage
- **WHEN** a newly created execution later fails
- **THEN** the previously recorded call-edge indexes SHALL remain queryable

### Requirement: Call-edge index updates SHALL be concurrency-safe and canonicalized
Each call-edge update SHALL perform a full read, merge, dedupe, and sort before a conditional ETag-checked write. On an ETag conflict, the runtime SHALL retry the full read/merge/write sequence until the write succeeds or the operation aborts.

#### Scenario: Concurrent writes preserve both callers
- **WHEN** two callers concurrently update the same reverse index for callee cache key `ck1`
- **THEN** the runtime SHALL retry conflicts until both callers are present in the stored lists

#### Scenario: Repeated edge writes remain canonical
- **WHEN** the same caller/callee edge is recorded more than once across retries or repeated new-execution attempts
- **THEN** the stored forward and reverse indexes SHALL remain deduped and sorted
