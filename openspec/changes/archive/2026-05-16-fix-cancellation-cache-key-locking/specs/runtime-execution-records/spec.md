## MODIFIED Requirements

### Requirement: Runtime SHALL separate cache identity from execution identity
The runtime SHALL treat `cache_key` as the stable computation identity and `execution_id` as the stable identity of one execution attempt. The runtime SHALL acquire execution coordination locks by `cache_key` for launch, resume, and cancellation, SHALL propagate `execution_id` in the adapter envelope, and SHALL use execution id as the identity for dependency edges, execution state objects, and invalidation records.

#### Scenario: First launch creates a new execution identity
- **WHEN** `start_fn` observes a cache miss and confirms there is no active execution for the computed `cache_key`
- **THEN** it creates a new `execution_id` for that launch attempt
- **AND** it invokes the adapter with both `cache_key` and `execution_id`

#### Scenario: Resume preserves the current execution identity
- **WHEN** `start_fn` observes an active execution for a `cache_key`
- **THEN** it SHALL reuse the referenced `execution_id`
- **AND** it SHALL NOT create a new `execution_id` for that execution while resuming it

#### Scenario: Cancellation resolves lock identity from the execution record
- **WHEN** cancellation targets execution `e1`
- **AND** `exec/state/e1.json` records `cache_key = "ck1"`
- **THEN** the runtime SHALL acquire the execution coordination lock for `ck1`
- **AND** it SHALL continue to use `e1` as the execution-record and dependency-graph identity

### Requirement: Runtime SHALL maintain one mutable execution object per execution id
The runtime SHALL persist `exec/state/<execution_id>.json` as the single compare-and-swap updated execution object for that execution. That object SHALL include `execution_id`, `cache_key`, `created_at`, `status`, `state`, `dependencies`, `updated_at`, and `cancel_requested_by`, where `cancel_requested_by` is `str | null`. `status` SHALL be one of `running`, `cancel-requested`, `cancelled`, `succeeded`, or `failed`. `state` SHALL contain the durable adapter state returned by the first adapter call for that execution and SHALL be `null` when no durable adapter state exists. Once `state` is first written for an execution, the runtime SHALL NOT replace or merge it on later updates. `dependencies` SHALL be the deduped set of discovered callee execution ids for that execution. Execution-object updates SHALL be monotone: newly discovered dependencies MAY be added, terminal status MAY replace non-terminal status, `cancel-requested` MAY precede `cancelled`, and existing dependencies SHALL NOT be removed.

The same execution-object schema SHALL also be used for each live index id. For index-root records, the object path SHALL be `exec/state/<index_id>.json`, `execution_id` SHALL equal the `index_id`, `cache_key` SHALL equal the `index_id`, `state` SHALL be `null`, and `dependencies` SHALL track the deduped set of execution ids started from that index.

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

#### Scenario: Cancellation requester is recorded before cancellation work
- **WHEN** a user requests cancellation for execution `e0`
- **THEN** the runtime SHALL update `exec/state/e0.json` so that `status = "cancel-requested"` before invoking adapter cancellation work for `e0`
- **AND** `cancel_requested_by` contains the requesting user identity

#### Scenario: Cancelled is per-execution cleanup completion
- **WHEN** the index-cancellation runtime persists `exec/state/e0.json` with `status = "cancelled"`
- **THEN** it SHALL mean cleanup for execution `e0` is complete
- **AND** it SHALL mean the index-cancellation runtime does not need to invoke adapter cancellation for `e0` again
- **AND** it SHALL NOT by itself mean the rooted execution graph is fully cancelled

#### Scenario: Terminal cancelled is owned by the index-cancellation runtime across the full adapter chain
- **WHEN** a cancellation update for execution `e0` returns progress from one adapter layer
- **AND** other adapter layers in the chain may still require cleanup
- **THEN** the runtime SHALL keep `exec/state/e0.json` at `status = "cancel-requested"`
- **AND** the index-cancellation runtime SHALL persist `status = "cancelled"` only after the full adapter chain has completed cancellation handling

#### Scenario: Execution object includes minimal execution fields
- **WHEN** the runtime persists `exec/state/e0.json`
- **THEN** that object SHALL contain `execution_id`, `cache_key`, `created_at`, `status`, `state`, `dependencies`, `updated_at`, and `cancel_requested_by`

#### Scenario: Execution object rejects unknown status values
- **WHEN** the runtime validates or persists `exec/state/e0.json`
- **THEN** `status` SHALL be one of `running`, `cancel-requested`, `cancelled`, `succeeded`, or `failed`

#### Scenario: Index id is persisted as a synthetic root execution
- **WHEN** runtime work is started from index `idx1`
- **THEN** the runtime SHALL maintain `exec/state/idx1.json`
- **AND** that object SHALL use `execution_id = "idx1"`, `cache_key = "idx1"`, and `state = null`

#### Scenario: Index root accumulates launched execution dependencies
- **WHEN** index `idx1` starts execution `e1`
- **THEN** the runtime SHALL update `exec/state/idx1.json` so that `dependencies` contains `e1`
