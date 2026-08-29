## MODIFIED Requirements

### Requirement: Runtime SHALL separate cache identity from execution identity
The runtime SHALL treat `cache_key` as the stable computation identity and `execution_id` as the stable identity of one execution attempt. The runtime SHALL acquire execution locks by `cache_key`, SHALL propagate `execution_id` in the adapter envelope, and SHALL use execution id as the identity for dependency edges, execution state objects, and invalidation records.

#### Scenario: First launch creates a new execution identity
- **WHEN** `start_fn` observes a cache miss and confirms there is no active execution for the computed `cache_key`
- **THEN** it creates a new `execution_id` for that launch attempt
- **AND** it invokes the adapter with both `cache_key` and `execution_id`

#### Scenario: Resume preserves the current execution identity
- **WHEN** `start_fn` observes an active execution for a `cache_key`
- **THEN** it SHALL reuse the referenced `execution_id`
- **AND** it SHALL NOT create a new `execution_id` for that execution while resuming it

### Requirement: Runtime SHALL maintain an active execution pointer per cache key
The runtime SHALL persist the currently active execution for a `cache_key` at `active/<cache_key>` as plain text containing only the `execution_id`.

#### Scenario: Active pointer is created for a new running execution
- **WHEN** the first adapter call for a new execution returns `running`
- **THEN** the runtime SHALL create `active/<cache_key>` containing that execution's `execution_id`

#### Scenario: Stale active pointer is discarded
- **WHEN** `active/<cache_key>` exists but `exec/state/<execution_id>.json` does not exist
- **THEN** the runtime SHALL delete `active/<cache_key>`
- **AND** it SHALL treat the cache key as having no active execution

### Requirement: Runtime SHALL maintain one mutable execution object per execution id
The runtime SHALL persist `exec/state/<execution_id>.json` as the single compare-and-swap updated execution object for that execution. That object SHALL include `execution_id`, `cache_key`, `created_at`, `status`, `state`, `dependencies`, `updated_at`, and `cancel_requested_by`, where `cancel_requested_by` is `str | null`. `status` SHALL be one of `running`, `cancel-requested`, `cancelled`, `succeeded`, or `failed`. `state` SHALL contain the durable adapter state returned by the first adapter call for that execution and SHALL be `null` when no durable adapter state exists. Once `state` is first written for an execution, the runtime SHALL NOT replace or merge it on later updates. `dependencies` SHALL be the deduped set of discovered callee execution ids for that execution. Execution-object updates SHALL be monotone: newly discovered dependencies MAY be added, terminal status MAY replace non-terminal status, `cancel-requested` MAY precede `cancelled`, and existing dependencies SHALL NOT be removed.

The execution-object schema SHALL be:

- `execution_id: str`
- `cache_key: str`
- `created_at: int`
- `status: "running" | "cancel-requested" | "cancelled" | "succeeded" | "failed"`
- `state: object | null`
- `dependencies: list[str]`
- `updated_at: int`
- `cancel_requested_by: str | null`

#### Scenario: First adapter call creates the execution object
- **WHEN** the first adapter call for a new execution returns any valid adapter result
- **THEN** the runtime SHALL create `exec/state/<execution_id>.json`
- **AND** that object SHALL contain the returned adapter `state` when one exists

#### Scenario: First execution object records creation time
- **WHEN** the runtime first creates `exec/state/<execution_id>.json`
- **THEN** that object SHALL contain `created_at`
- **AND** `created_at` SHALL remain unchanged on later updates

#### Scenario: Resume uses stored execution state
- **WHEN** `start_fn` resumes an active execution
- **THEN** it SHALL load the adapter `state` from `exec/state/<execution_id>.json`
- **AND** it SHALL pass that stored state to the adapter

#### Scenario: Later running result does not replace stored execution state
- **WHEN** the runtime invokes an adapter for an existing execution and the adapter returns `running` with durable `state`
- **THEN** the runtime SHALL keep the existing stored `state` in `exec/state/<execution_id>.json`

#### Scenario: Late dependency discovery expands execution summary
- **WHEN** execution `e0` later discovers a dependency on execution `e1`
- **THEN** the runtime SHALL update `exec/state/e0.json` so that `dependencies` contains `e1`

#### Scenario: Dependency merge survives compare-and-swap retry
- **WHEN** a compare-and-swap update to `exec/state/e0.json` observes a conflicting write
- **THEN** the runtime SHALL reread, merge the dependency set and monotone status fields, and retry the conditional write

#### Scenario: Cancellation requester is recorded on cancel request
- **WHEN** a user requests cancellation for execution `e0`
- **THEN** the runtime SHALL update `exec/state/e0.json` so that `status = "cancel-requested"`
- **AND** `cancel_requested_by` contains the requesting user identity

#### Scenario: Execution object includes minimal execution fields
- **WHEN** the runtime persists `exec/state/e0.json`
- **THEN** that object SHALL contain `execution_id`, `cache_key`, `created_at`, `status`, `state`, `dependencies`, `updated_at`, and `cancel_requested_by`

#### Scenario: Execution object rejects unknown status values
- **WHEN** the runtime validates or persists `exec/state/e0.json`
- **THEN** `status` SHALL be one of `running`, `cancel-requested`, `cancelled`, `succeeded`, or `failed`

### Requirement: Cache refs SHALL remain proper refs and record execution ids
The runtime SHALL publish `refs/cache/<cache_key>.json` as a normal cache ref to the current manifest for that cache key, and that ref SHALL also record `execution_id` for the current execution. Readers that materialize cached results SHALL continue resolving the cached manifest through the ref target, and graph planners SHALL read `execution_id` from the same cache ref.

#### Scenario: Successful execution updates cache pointer
- **WHEN** execution `e7` becomes the terminal cached result for cache key `ck1`
- **THEN** the runtime SHALL write `refs/cache/ck1.json` with `execution_id = "e7"`
- **AND** that object SHALL remain a valid cache ref with its manifest `target`

#### Scenario: Re-run requires prior invalidation
- **WHEN** a later execution `e8` attempts to publish a terminal cached result for cache key `ck1`
- **AND** `refs/cache/ck1.json` already exists for an earlier execution
- **THEN** the runtime SHALL reject that cache publication
- **AND** the earlier cache ref MUST be invalidated or deleted before `e8` can publish `refs/cache/ck1.json`

### Requirement: Adapter envelope and result schema SHALL follow the runtime-owned execution contract
The adapter envelope SHALL include `argv_ptr`, `cache_key`, `execution_id`, `remote`, `runnable`, and `state`. The adapter result SHALL use only `running`, `succeeded`, or `failed` statuses. `running` MUST include durable `state`. `succeeded` MUST include `dag_id`. `failed` MUST include `error`.

#### Scenario: First adapter call uses null state
- **WHEN** the runtime invokes an adapter for a new execution
- **THEN** the adapter envelope SHALL include `state = null`

#### Scenario: Later adapter state is ignored after first write
- **WHEN** the runtime invokes an adapter for an existing execution and the adapter returns `running` with a different `state`
- **THEN** the runtime SHALL continue using the existing stored `state` from `exec/state/<execution_id>.json`

### Requirement: Failed execution SHALL be cached as a terminal result
If an adapter returns `failed`, the runtime SHALL complete the DAG with the error and SHALL publish that failed terminal outcome to cache for the `cache_key`.

#### Scenario: Failed adapter result populates cache
- **WHEN** an adapter returns `failed` for a cache key
- **THEN** the runtime SHALL complete the DAG with the reported error
- **AND** it SHALL publish the failed outcome into cache for that cache key

#### Scenario: Failed execution clears active pointer
- **WHEN** an active execution returns `failed`
- **THEN** the runtime SHALL delete `active/<cache_key>` before surfacing the failure
