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
Phase 1 SHALL determine the complete cancellation set before Phase 2 begins. For each reachable execution, it SHALL read the lifecycle and valid caller references while holding the execution's coordination lock. It SHALL skip executions that are terminal or retain a valid caller reference. It SHALL use compare-and-swap to transition each remaining active execution to `cancel-pending`; on a conflict it SHALL reread lifecycle and caller references, retry an active unreferenced execution, and skip an execution that became terminal. After selecting an execution, Phase 1 SHALL conditionally delete its matching cache pointer, enqueue its spawned executions, and idempotently remove its outgoing caller edges. It SHALL perform no adapter invocation.

#### Scenario: Planning completes before adapter work
- **WHEN** Phase 1 processes a rooted cancellation work set
- **THEN** it SHALL determine every reachable execution selected as `cancel-pending` before Phase 2 begins
- **AND** no invoke or cancel adapter operation is sent during Phase 1

#### Scenario: Referenced execution is not selected
- **WHEN** a reachable execution retains at least one valid caller reference
- **THEN** Phase 1 SHALL leave its lifecycle unchanged
- **AND** it SHALL NOT traverse or remove that execution's outgoing caller edges as part of that branch

#### Scenario: Terminal race is harmless
- **WHEN** an active cancellation candidate becomes `succeeded`, `failed`, or `canceled` before the `cancel-pending` compare-and-swap succeeds
- **THEN** Phase 1 SHALL skip that execution without raising an execution-status error
- **AND** it SHALL continue processing the remaining work set

#### Scenario: Selected execution relinquishes dependencies
- **WHEN** Phase 1 successfully selects execution `e1` as `cancel-pending`
- **THEN** it SHALL enqueue every execution in `e1.spawned_execution_ids`
- **AND** it SHALL idempotently remove every caller edge owned by `e1`
- **AND** it SHALL conditionally delete the cache pointer only when it still names `e1`

### Requirement: Cancellation Phase 2 SHALL cancel the selected set directly
Phase 2 SHALL begin only after Phase 1 has determined the complete cancellation set. It SHALL invoke the cancel adapter for each selected execution using that execution's persisted cancellation metadata and adapter inputs, then use compare-and-swap to transition its lifecycle from `cancel-pending` to `canceled`. Phase 2 SHALL NOT use an intermediate readiness lifecycle or readiness timeout. Executions already observed as `cancel-pending` SHALL be eligible for resumed Phase 1 reconstruction and Phase 2 processing.

#### Scenario: Selected adapter work advances directly to canceled
- **WHEN** Phase 2 processes a selected `cancel-pending` execution
- **THEN** it SHALL invoke that execution's applicable cancel adapter
- **AND** it SHALL compare-and-swap the lifecycle directly to `canceled`

#### Scenario: Interrupted planning is resumable
- **WHEN** a cancellation attempt stops after persisting `cancel-pending`
- **THEN** a later cancellation drive SHALL reconstruct the reachable selected work from persisted execution records
- **AND** it SHALL idempotently repeat Phase 1 cleanup before Phase 2

#### Scenario: Phase 2 completion conflicts
- **WHEN** the compare-and-swap from `cancel-pending` to `canceled` conflicts
- **THEN** Phase 2 SHALL reread the execution record
- **AND** it SHALL accept an already-terminal lifecycle or retry an execution that remains `cancel-pending`

### Requirement: ExecutionState SHALL expose a public mutation lifecycle guard
The runtime SHALL expose `ExecutionState.require_mutation(execution_id, db, mode="activation" | "mutation")` as the canonical public guard for mutation eligibility. The guard SHALL read `execution/<execution_id>`, classify the stored lifecycle for the requested mode, and either return the execution record unchanged or raise a typed execution-status error.

For `mode = "activation"`, only `lifecycle = "pending"` SHALL be accepted.

For `mode = "mutation"`, only `lifecycle = "running"` SHALL be accepted.

If the lifecycle is `cancel-pending` or `canceled`, the guard SHALL raise `CanceledExecutionError`. It SHALL NOT permit index or execution-result mutation after `cancel-pending` is persisted.

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

#### Scenario: Cancel-pending blocks mutation
- **WHEN** the guard reads `cancel-pending`
- **THEN** it SHALL raise `CanceledExecutionError`
- **AND** it SHALL NOT persist the attempted mutation

#### Scenario: Canceled blocks mutation
- **WHEN** the guard reads `canceled`
- **THEN** it SHALL raise `CanceledExecutionError`
- **AND** it SHALL NOT invoke adapter cancellation

### Requirement: Execution coordination retries SHALL not silently abandon CAS mutations
For child registration, terminal-child bookkeeping, Phase 1 cancellation selection, and Phase 2 cancellation completion, the execution-state layer SHALL retry compare-and-swap conflicts with bounded exponential backoff. Each cancellation retry SHALL reread the lifecycle and any caller-reference state required for its decision. Retry exhaustion SHALL be returned to the calling workflow as an error rather than being logged and treated as success.

#### Scenario: Registration conflict is retried from the latest record
- **WHEN** a child-registration CAS update conflicts
- **THEN** the runtime SHALL reread the caller execution record before retrying
- **AND** it SHALL evaluate the latest lifecycle before attempting the next update

#### Scenario: Cancellation conflict is re-evaluated
- **WHEN** a cancellation lifecycle compare-and-swap conflicts
- **THEN** the runtime SHALL reread the execution lifecycle before retrying
- **AND** Phase 1 SHALL also reread valid caller references before selecting the execution

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
