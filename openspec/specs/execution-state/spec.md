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
When cancellation leaves an execution with no remaining live callers, the runtime SHALL acquire the coordination lock for that execution's `cache_key`, recheck that no live callers remain, ensure the execution is not terminal, and remove `active/<cache_key>` before marking cancellation intent on lifecycle state.

#### Scenario: Orphaned callee loses active pointer before cancellation lifecycle update
- **WHEN** cancellation removes the last live caller edge for callee execution `e1`
- **THEN** the runtime SHALL lock the coordination key for `e1`'s `cache_key`
- **AND** it SHALL delete `active/<cache_key>` before setting the callee lifecycle to a `cancel-*` value

#### Scenario: New caller relaunches after detached cancellation
- **WHEN** a later caller computes the same `cache_key` after the prior execution was cancellation-detached and `active/<cache_key>` is absent
- **THEN** the runtime SHALL treat the computation as having no current execution
- **AND** it SHALL create a fresh execution attempt instead of resuming the detached one
