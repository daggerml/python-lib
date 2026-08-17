## Purpose
Define remote execution coordination, lifecycle guards, and mutation serialization.

## Requirements

### Requirement: ExecutionState constructed from remote_root
The system SHALL accept `remote_root: str` as a required configuration parameter for `ExecutionState`. Call sites that construct `ExecutionState` MUST provide a valid remote root explicitly and MUST NOT rely on optional remote-root values or `None` defaults.

#### Scenario: remote_root parsed to bucket and prefix
- **WHEN** `ExecutionState(remote_root="s3://my-bucket/my/prefix")` is constructed
- **THEN** execution-record and cache-pointer operations target that bucket and prefix

#### Scenario: call site provides explicit remote_root
- **WHEN** code constructs `ExecutionState` for a remote-backed execution flow
- **THEN** that call site passes a concrete `remote_root: str` value at construction time

#### Scenario: optional or None remote_root defaults are not relied on
- **WHEN** a remote-backed execution flow constructs `ExecutionState`
- **THEN** it does not rely on an optional remote-root parameter or a `None` default to supply remote configuration

### Requirement: Cancellation Phase 1 SHALL not invoke adapters
Phase 1 SHALL only plan cancellation, update the locked execution record, conditionally delete its cache pointer, remove applicable caller edges, and enqueue direct callees. It SHALL perform no adapter invocation.

#### Scenario: Planning completes without adapter work
- **WHEN** Phase 1 processes a cancellation work set
- **THEN** no invoke or cancel adapter operation is sent

### Requirement: Cancellation Phase 2 SHALL be distributed and leaves-first
Each runtime handling a `cancel-requested` execution SHALL wait for its direct callees to reach `cancel-ready`, invoke cancellation for those callees using `argv_ref` from their execution records, persist those callees as `canceled`, and then persist its own execution as `cancel-ready`. Every execution-record transition SHALL occur under that record's embedded lock. The wait SHALL time out after 60 seconds, after which the runtime SHALL perform the cancel-adapter work anyway.

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
The runtime SHALL expose `ExecutionState.require_mutation(execution_id, db, mode="activation" | "mutation")` as the canonical public guard for mutation eligibility. The guard SHALL read `execution/<execution_id>`, classify the stored lifecycle for the requested mode, and either return the execution record unchanged or raise a typed execution-status error.

For `mode = "activation"`, only `lifecycle = "pending"` SHALL be accepted.

For `mode = "mutation"`, only `lifecycle = "running"` SHALL be accepted.

If the lifecycle is `cancel-requested`, the guard SHALL drive Phase 2 cancellation before raising `CanceledExecutionError`.

If the lifecycle is `cancel-ready` or `canceled`, the guard SHALL raise `CanceledExecutionError` without driving cancellation.

If the lifecycle is any other non-accepted value for the requested mode, the guard SHALL raise `BadExecutionStatusError`.

#### Scenario: Activation guard accepts pending execution
- **WHEN** `ExecutionState.require_mutation("e1", db, mode="activation")` reads `execution/e1`
- **AND** the lifecycle is `pending`
- **THEN** it returns the stored execution record without mutation

#### Scenario: Mutation guard accepts running execution
- **WHEN** `ExecutionState.require_mutation("e1", db, mode="mutation")` reads `execution/e1`
- **AND** the lifecycle is `running`
- **THEN** it returns the stored execution record without mutation

#### Scenario: Activation guard rejects non-pending non-cancel states
- **WHEN** `ExecutionState.require_mutation("e1", db, mode="activation")` reads `execution/e1`
- **AND** the lifecycle is `running`, `succeeded`, or `failed`
- **THEN** it raises `BadExecutionStatusError`

#### Scenario: Mutation guard rejects non-running non-cancel states
- **WHEN** `ExecutionState.require_mutation("e1", db, mode="mutation")` reads `execution/e1`
- **AND** the lifecycle is `pending`, `succeeded`, or `failed`
- **THEN** it raises `BadExecutionStatusError`

#### Scenario: Cancel-requested drives cancellation before raising
- **WHEN** `ExecutionState.require_mutation("e1", db, mode="activation")` or `ExecutionState.require_mutation("e1", db, mode="mutation")` reads `execution/e1`
- **AND** the lifecycle is `cancel-requested`
- **THEN** it drives Phase 2 cancellation for `e1`
- **AND** it raises `CanceledExecutionError`

#### Scenario: Terminal cancel states raise without driving
- **WHEN** `ExecutionState.require_mutation("e1", db, mode="activation")` or `ExecutionState.require_mutation("e1", db, mode="mutation")` reads `execution/e1`
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

### Requirement: Execution mutations SHALL be serialized by embedded owner locks
Each `execution/<execution_id>` record SHALL contain `lock = null` or `lock = {owner: str, ttl: float}`. Lock acquisition SHALL use compare-and-swap to replace a null or expired lock with a fresh UUID4 owner. Every execution-record mutation other than lock acquisition SHALL require the current lock owner and SHALL use compare-and-swap against the latest ETag. Unlock SHALL compare-and-swap the lock to null only when the stored owner matches the unlocking owner.

#### Scenario: One caller acquires an unlocked execution
- **WHEN** two callers concurrently attempt to replace a null execution lock
- **THEN** exactly one conditional update succeeds
- **AND** the successful record stores that caller's owner UUID

#### Scenario: Stale owner cannot mutate after a steal
- **WHEN** an expired lock owned by `o1` is conditionally replaced by owner `o2`
- **THEN** a mutation from `o1` fails its stale compare-and-swap
- **AND** `o1` stops after rereading owner `o2`

#### Scenario: Stale unlock preserves replacement owner
- **WHEN** owner `o1` attempts to unlock after owner `o2` has stolen the lock
- **THEN** the runtime SHALL NOT clear `o2`'s lock

### Requirement: Lock expiry SHALL use S3 response time
The runtime SHALL determine lock expiry using `LastModified + lock.ttl <= Date`, where `LastModified` and HTTP `Date` come from the same S3 execution-record response. It SHALL NOT use caller wall-clock time for that decision. Expiry SHALL permit lock stealing but SHALL NOT revoke an unchanged owner by itself.

#### Scenario: S3 timestamps report an expired lock
- **WHEN** an execution response has `LastModified + lock.ttl <= Date`
- **THEN** another caller MAY attempt to replace the lock owner by compare-and-swap

#### Scenario: Expired owner remains authoritative until stolen
- **WHEN** an adapter response arrives after the lock TTL
- **AND** the execution record still contains the caller's owner UUID
- **THEN** that caller MAY persist the response by compare-and-swap

#### Scenario: Owner mutation refreshes lease basis
- **WHEN** the lock owner successfully mutates the execution record
- **THEN** S3 updates `LastModified`
- **AND** subsequent expiry checks SHALL use that updated timestamp

### Requirement: Cache resolution SHALL coordinate one current execution
On a cache miss, the runtime SHALL create a fresh UUID7 execution record with a fresh owner lock before conditionally creating `cache/<cache_key>` containing only that execution ID. If cache-pointer creation conflicts, the runtime SHALL conditionally delete only its unchanged new execution record and SHALL reread the winning cache pointer. UUID ordering SHALL NOT select the winner.

#### Scenario: Concurrent cache miss has one winner
- **WHEN** multiple callers create different execution records for one absent cache key
- **THEN** S3 conditional cache-pointer creation selects exactly one current execution
- **AND** losing callers reread that winner

#### Scenario: Execution exists before pointer publication
- **WHEN** a caller successfully creates `cache/ck1` containing `e1`
- **THEN** `execution/e1` already exists

#### Scenario: Lost claim cleans only the losing record
- **WHEN** execution `e2` loses cache-pointer creation to execution `e1`
- **THEN** the `e2` caller conditionally deletes its unchanged execution record
- **AND** it does not modify `e1`
