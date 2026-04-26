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
`IndexOps.start_fn` SHALL implement the following flow on every call:
1. Check cache — return node if hit.
2. Attempt `lock()` — return `None` if failed.
3. Recheck cache — if hit, delete lock file and return node.
4. Call adapter (must return quickly); adapter stdout carries `{status, dag_id?, error?}`.
5. On `succeeded`: publish result to cache, delete lock file.
6. On `failed`: delete lock file, raise.
7. On `running`: delete lock file, return `None`.

#### Scenario: Cache hit before lock returns node immediately
- **WHEN** `start_fn` is called and the cache already contains a result
- **THEN** the node is returned without acquiring the lock

#### Scenario: Lock contention returns None
- **WHEN** `start_fn` is called and another process holds the lock
- **THEN** `None` is returned so the caller retries

#### Scenario: Cache hit after lock cleans up and returns node
- **WHEN** `start_fn` acquires the lock but finds a cache hit on recheck
- **THEN** the lock file is deleted and the cached node is returned

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
The system SHALL accept `remote_root: str` as its sole configuration parameter.

#### Scenario: remote_root parsed to bucket and prefix
- **WHEN** `ExecutionState(cache_key, remote_root="s3://my-bucket/my/prefix")` is constructed
- **THEN** lock operations target `s3://my-bucket/my/prefix/exec/{cache_key}.json`

#### Scenario: Missing remote_root raises error
- **WHEN** `ExecutionState` is constructed without a valid `remote_root`
- **THEN** a `DmlRepoError` is raised with a descriptive message
