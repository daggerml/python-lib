### Requirement: S3-backed mutex lock file
The system SHALL store a lock file at `{remote_root_prefix}/exec/{cache_key}.json` containing only `{lock_token: str, lock_expires_ts: float}`. No status, metadata, or job-specific fields.

#### Scenario: Lock file written to correct S3 key
- **WHEN** `ExecutionState(cache_key, remote_root="s3://bucket/prefix").lock()` succeeds
- **THEN** a JSON object is written to `s3://bucket/prefix/exec/{cache_key}.json`

#### Scenario: No DynamoDB dependency
- **WHEN** any `ExecutionState` method is called
- **THEN** no DynamoDB client is created and `DML_DYNAMODB_TABLE` is not read

### Requirement: Lock acquired via create-if-absent
The system SHALL acquire the lock by PUT with `If-None-Match: *`. If the object already exists and its `lock_expires_ts` has not passed, `lock()` SHALL return `False`. If the existing lock is expired, the system SHALL DELETE it and re-PUT, returning `True`.

#### Scenario: Lock acquired when no file exists
- **WHEN** `lock()` is called and no lock file exists at the key
- **THEN** the file is created with a fresh `lock_token` and `lock_expires_ts`, and `True` is returned

#### Scenario: Lock refused when held and not expired
- **WHEN** `lock()` is called and a non-expired lock file exists
- **THEN** `False` is returned and the file is unchanged

#### Scenario: Expired lock is stolen
- **WHEN** `lock()` is called and an expired lock file exists
- **THEN** the old file is deleted, a new one is created, and `True` is returned

#### Scenario: Concurrent create conflict returns False
- **WHEN** the `If-None-Match: *` PUT returns `412 PreconditionFailed`
- **THEN** `False` is returned without raising

### Requirement: Lock released via DELETE
The system SHALL release the lock by DELETE of the lock file. No updates to the file are ever made.

#### Scenario: Unlock deletes the file
- **WHEN** `unlock()` is called by the lock holder
- **THEN** the lock file is deleted from S3

#### Scenario: Unlock is idempotent
- **WHEN** `unlock()` is called and the file does not exist
- **THEN** no error is raised

### Requirement: start_fn mutex-gated adapter dispatch
`IndexOps.start_fn` SHALL implement the following flow on every adapter-backed call:
1. Check cache and return the DAG if hit.
2. Attempt `lock()` and return `None` if it fails.
3. Recheck cache and return the DAG if hit.
4. If no active execution exists for the cache key, reserve a fresh `execution_id` by creating its execution record with `lifecycle = "pending"`.
5. Publish or reuse `active/<cache_key>` only for an execution id that already has an execution record.
6. Record caller/callee dependency state and call the adapter with the active execution id.
7. On terminal success or failure, publish terminal DAG state, clean up the active pointer, and release the lock.
8. On `running`, persist or update launch state, release the lock, and return `None`.

#### Scenario: Cache hit before lock returns node immediately
- **WHEN** `start_fn` is called and the cache already contains a result
- **THEN** the node is returned without acquiring the lock

#### Scenario: Lock contention returns None
- **WHEN** `start_fn` is called and another process holds the lock
- **THEN** `None` is returned so the caller retries

#### Scenario: Cache hit after lock cleans up and returns node
- **WHEN** `start_fn` acquires the lock but finds a cache hit on recheck
- **THEN** the lock file is deleted and the cached node is returned

#### Scenario: Fresh launch reserves pending execution before active publication
- **WHEN** `start_fn` observes a cache miss and no active execution for cache key `ck1`
- **THEN** it SHALL create `exec/state/<execution_id>.json` with `lifecycle = "pending"` before publishing `active/ck1`

#### Scenario: Active execution always has a backing execution record
- **WHEN** `start_fn` publishes or reuses `active/ck1`
- **THEN** the referenced `execution_id` SHALL already have `exec/state/<execution_id>.json`

#### Scenario: Missing execution record behind active pointer is stale
- **WHEN** `active/ck1` points at execution `e1`
- **AND** `exec/state/e1.json` does not exist
- **THEN** the runtime SHALL treat `active/ck1` as stale coordination state
- **AND** it SHALL delete or replace that active pointer before continuing normal launch or resume behavior

#### Scenario: Adapter success publishes cache and releases lock
- **WHEN** the adapter returns `status: succeeded` with a `dag_id`
- **THEN** the result is published to cache and the lock file is deleted

#### Scenario: Adapter failure releases lock and raises
- **WHEN** the adapter returns `status: failed`
- **THEN** the lock file is deleted and a `DmlRepoError` is raised

#### Scenario: Adapter still running releases lock and returns None
- **WHEN** the adapter returns `status: running`
- **THEN** the lock file is deleted and `None` is returned

### Requirement: ExecutionState constructed from remote_root
The system SHALL accept `remote_root: str` as a required configuration parameter for `ExecutionState`. Call sites that construct `ExecutionState` MUST provide a valid remote root explicitly and MUST NOT rely on optional remote-root values or `None` defaults.

#### Scenario: remote_root parsed to bucket and prefix
- **WHEN** `ExecutionState(cache_key, remote_root="s3://my-bucket/my/prefix")` is constructed
- **THEN** lock operations target `s3://my-bucket/my/prefix/exec/{cache_key}.json`

#### Scenario: call site provides explicit remote_root
- **WHEN** code constructs `ExecutionState` for a remote-backed execution flow
- **THEN** that call site passes a concrete `remote_root: str` value at construction time

#### Scenario: optional or None remote_root defaults are not relied on
- **WHEN** a remote-backed execution flow constructs `ExecutionState`
- **THEN** it does not rely on an optional remote-root parameter or a `None` default to supply remote configuration

### Requirement: Caller-owned launch state SHALL be serialized by cache-key lock
The runtime SHALL persist caller-owned `launch_state` for each execution attempt separately from lifecycle state. `launch_state` SHALL contain `execution_id`, `cache_key`, `resume_state`, and `created_at`. The runtime SHALL create and update `launch_state` only while holding the coordination lock for the corresponding `cache_key`.

#### Scenario: First running launch persists launch state under lock
- **WHEN** `start_fn` launches a new execution and receives a `running` adapter result with durable resume data
- **THEN** it SHALL persist `launch_state` containing `execution_id`, `cache_key`, `resume_state`, and `created_at`
- **AND** it SHALL do so while holding the lock for that `cache_key`

#### Scenario: Resume reads launch state under lock
- **WHEN** `start_fn` resumes an execution referenced by `active/<cache_key>`
- **THEN** it SHALL read that execution's `launch_state` while holding the lock for that `cache_key`
- **AND** it SHALL pass `resume_state` from `launch_state` to the adapter

### Requirement: Cancellation orphaning SHALL remove current-execution ownership under lock
When Phase 1 cancellation processes an execution ID, the runtime SHALL acquire the coordination lock for that execution's `cache_key`, retrying until acquired. If the execution has live callers, processing that ID SHALL stop without changing its lifecycle or active ownership. Otherwise, the runtime SHALL set lifecycle to `cancel-requested`, remove each direct caller/callee edge, and move `refs/active/<cache_key>.json` to `refs/cancel-targets/<execution_id>.json` while conditionally verifying that the active ref still names that execution. The move SHALL preserve the existing argv manifest without regeneration. Direct callees SHALL then be added to the Phase 1 work set.

#### Scenario: User cancellation starts with explicit execution IDs
- **WHEN** a user requests cancellation for execution IDs `e1` and `e2`
- **THEN** Phase 1 initializes its work set with exactly those IDs

#### Scenario: Live callers prevent cancellation planning
- **WHEN** Phase 1 pops `e1`
- **AND** `e1` still has live callers
- **THEN** Phase 1 stops processing `e1`
- **AND** it does not mark `e1` cancel-requested

#### Scenario: Orphaned execution moves active ownership to a cancel target
- **WHEN** Phase 1 processes orphaned execution `e1` for cache key `ck1`
- **AND** `active/ck1` names `e1`
- **THEN** it marks `e1` as `cancel-requested`
- **AND** it removes `e1` as caller from each direct callee before planning those callees
- **AND** it moves the existing active manifest to `cancel-targets/e1`
- **AND** it does not regenerate the argv manifest

#### Scenario: Active ref rebinding is not overwritten
- **WHEN** Phase 1 processes `e1`
- **AND** `active/ck1` names a different execution
- **THEN** it does not move or delete that active ref

### Requirement: Cancellation Phase 1 SHALL not invoke adapters
Phase 1 SHALL only plan cancellation, update lifecycle state, move active ownership, and enqueue direct callees. It SHALL perform no adapter invocation.

#### Scenario: Planning completes without adapter work
- **WHEN** Phase 1 processes a cancellation work set
- **THEN** no invoke or cancel adapter operation is sent

### Requirement: Cancellation Phase 2 SHALL be distributed and leaves-first
Each runtime handling a `cancel-requested` execution SHALL wait for its direct callees to reach `cancel-ready`, invoke cancellation for those callees using their cancel-target refs, persist those callees as `canceled`, and then persist its own execution as `cancel-ready`. The wait SHALL time out after 60 seconds, after which the runtime SHALL perform the cancel-adapter work anyway.

#### Scenario: Parent waits for callees
- **WHEN** a cancel-requested execution has a callee that is not `cancel-ready`
- **THEN** its runtime does not yet invoke that callee's cancel adapter

#### Scenario: Leaf-first cleanup advances the parent
- **WHEN** all direct callees of `e1` are `cancel-ready`
- **THEN** the runtime invokes their cancel adapters
- **AND** it marks those callees `canceled`
- **AND** it marks `e1` `cancel-ready`

#### Scenario: Readiness timeout forces cleanup
- **WHEN** an execution remains `cancel-ready` for more than 60 seconds without normal handoff cleanup
- **THEN** a runtime invokes the applicable cancel adapters anyway
- **AND** the cleanup path remains safe to retry

### Requirement: ExecutionState SHALL expose a public mutation lifecycle guard
The runtime SHALL expose `ExecutionState.require_mutation(execution_id, db, mode="activation" | "mutation")` as the canonical public guard for mutation eligibility. The guard SHALL read `exec/state/<execution_id>.json`, classify the stored lifecycle for the requested mode, and either return the execution record unchanged or raise a typed execution-status error.

For `mode = "activation"`, only `lifecycle = "pending"` SHALL be accepted.

For `mode = "mutation"`, only `lifecycle = "running"` SHALL be accepted.

If the lifecycle is `cancel-requested`, the guard SHALL drive Phase 2 cancellation before raising `CanceledExecutionError`.

If the lifecycle is `cancel-ready` or `canceled`, the guard SHALL raise `CanceledExecutionError` without driving cancellation.

If the lifecycle is any other non-accepted value for the requested mode, the guard SHALL raise `BadExecutionStatusError`.

#### Scenario: Activation guard accepts pending execution
- **WHEN** `ExecutionState.require_mutation("e1", db, mode="activation")` reads `exec/state/e1.json`
- **AND** the lifecycle is `pending`
- **THEN** it returns the stored execution record without mutation

#### Scenario: Mutation guard accepts running execution
- **WHEN** `ExecutionState.require_mutation("e1", db, mode="mutation")` reads `exec/state/e1.json`
- **AND** the lifecycle is `running`
- **THEN** it returns the stored execution record without mutation

#### Scenario: Activation guard rejects non-pending non-cancel states
- **WHEN** `ExecutionState.require_mutation("e1", db, mode="activation")` reads `exec/state/e1.json`
- **AND** the lifecycle is `running`, `succeeded`, or `failed`
- **THEN** it raises `BadExecutionStatusError`

#### Scenario: Mutation guard rejects non-running non-cancel states
- **WHEN** `ExecutionState.require_mutation("e1", db, mode="mutation")` reads `exec/state/e1.json`
- **AND** the lifecycle is `pending`, `succeeded`, or `failed`
- **THEN** it raises `BadExecutionStatusError`

#### Scenario: Cancel-requested drives cancellation before raising
- **WHEN** `ExecutionState.require_mutation("e1", db, mode="activation")` or `ExecutionState.require_mutation("e1", db, mode="mutation")` reads `exec/state/e1.json`
- **AND** the lifecycle is `cancel-requested`
- **THEN** it drives Phase 2 cancellation for `e1`
- **AND** it raises `CanceledExecutionError`

#### Scenario: Terminal cancel states raise without driving
- **WHEN** `ExecutionState.require_mutation("e1", db, mode="activation")` or `ExecutionState.require_mutation("e1", db, mode="mutation")` reads `exec/state/e1.json`
- **AND** the lifecycle is `cancel-ready` or `canceled`
- **THEN** it raises `CanceledExecutionError`

### Requirement: Execution coordination retries SHALL not silently abandon CAS mutations
For child-registration and terminal-child bookkeeping mutations, the execution-state layer SHALL retry compare-and-swap conflicts with bounded exponential backoff. Retry exhaustion SHALL be returned to the calling workflow as an error rather than being logged and treated as success.

#### Scenario: Registration conflict is retried from the latest record
- **WHEN** a child-registration CAS update conflicts
- **THEN** the runtime SHALL reread the caller execution record before retrying
- **AND** it SHALL evaluate the latest lifecycle before attempting the next update

#### Scenario: Exhaustion is observable to the caller
- **WHEN** a bounded execution-record coordination retry budget is exhausted
- **THEN** the execution-state layer SHALL raise an error to its caller
- **AND** it SHALL not return a successful coordination result
