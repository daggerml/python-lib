### Requirement: Runtime SHALL separate cache identity from execution identity
The runtime SHALL treat `cache_key` as the stable computation identity and `execution_id` as the stable identity of one in-flight execution attempt. The runtime SHALL acquire execution locks by `cache_key` and SHALL propagate `execution_id` in the adapter envelope.

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
- **WHEN** `active/<cache_key>` exists but `exec/<execution_id>.json` does not exist
- **THEN** the runtime SHALL delete `active/<cache_key>`
- **AND** it SHALL treat the cache key as having no active execution

### Requirement: Runtime SHALL create immutable execution records
The runtime SHALL persist `exec/<execution_id>.json` only on the first non-terminal adapter result for an execution. That record SHALL contain the `execution_id`, the `cache_key`, the terminal-or-running status captured at creation time, and the durable adapter state returned from the first launch call. The runtime SHALL NOT modify that record after creation.

#### Scenario: First running result creates the execution record
- **WHEN** the first adapter call for a new execution returns `running` with durable state
- **THEN** the runtime SHALL create `exec/<execution_id>.json` containing that state
- **AND** it SHALL NOT rewrite that object on later resumes

#### Scenario: Resume uses stored immutable state
- **WHEN** `start_fn` resumes an active execution
- **THEN** it SHALL load the adapter `state` from `exec/<execution_id>.json`
- **AND** it SHALL pass that stored state to the adapter

### Requirement: Adapter envelope and result schema SHALL follow the runtime-owned execution contract
The adapter envelope SHALL include `argv_ptr`, `cache_key`, `execution_id`, `remote`, `runnable`, and `state`. The adapter result SHALL use only `running`, `succeeded`, or `failed` statuses. `running` MUST include durable `state`. `succeeded` MUST include `dag_id`. `failed` MUST include `error`.

#### Scenario: First adapter call uses null state
- **WHEN** the runtime invokes an adapter for a new execution
- **THEN** the adapter envelope SHALL include `state = null`

#### Scenario: Later adapter state is ignored
- **WHEN** the runtime invokes an adapter for an existing execution and the adapter returns `running` with a different `state`
- **THEN** the runtime SHALL ignore the returned replacement state
- **AND** it SHALL continue to treat the original execution record as authoritative

#### Scenario: Pending is rejected
- **WHEN** an adapter returns `pending`
- **THEN** the runtime SHALL reject that result as invalid adapter output

### Requirement: Stale lock recovery SHALL preserve active execution ownership
The runtime SHALL use the lock for `cache_key` only to coordinate mutation of the active execution. If a lock is stale and an active execution record exists, the runtime SHALL recover the lock and resume that execution instead of creating a new one.

#### Scenario: Stale lock with active execution resumes existing execution
- **WHEN** the lock for a `cache_key` is stale and `active/<cache_key>` points to an existing execution record
- **THEN** the runtime SHALL recover the lock
- **AND** it SHALL resume the existing `execution_id`
- **AND** it SHALL NOT launch a duplicate execution

### Requirement: Failed execution SHALL be cached as a terminal result
If an adapter returns `failed`, the runtime SHALL complete the DAG with the error and SHALL publish that failed terminal outcome to cache for the `cache_key`.

#### Scenario: Failed adapter result populates cache
- **WHEN** an adapter returns `failed` for a cache key
- **THEN** the runtime SHALL complete the DAG with the reported error
- **AND** it SHALL publish the failed outcome into cache for that cache key

#### Scenario: Failed execution clears active pointer
- **WHEN** an active execution returns `failed`
- **THEN** the runtime SHALL delete `active/<cache_key>` before surfacing the failure
