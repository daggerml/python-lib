## MODIFIED Requirements

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
- **WHEN** the runtime invokes an adapter for an existing execution and the adapter returns `running` with a different `state`
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

#### Scenario: Index id is persisted as a synthetic root execution
- **WHEN** runtime work is started from index `idx1`
- **THEN** the runtime SHALL maintain `exec/state/idx1.json`
- **AND** that object SHALL use `execution_id = "idx1"`, `cache_key = "idx1"`, and `state = null`

#### Scenario: Index root accumulates launched execution dependencies
- **WHEN** index `idx1` starts execution `e1`
- **THEN** the runtime SHALL update `exec/state/idx1.json` so that `dependencies` contains `e1`

### Requirement: Adapter envelope and result schema SHALL follow the runtime-owned execution contract
The adapter envelope SHALL include `argv_ptr`, `cache_key`, `execution_id`, `remote`, `runnable`, `state`, `execution_status`, and `cancel_requested_by`. The adapter result SHALL use only `running`, `succeeded`, `failed`, or `cancelled` statuses. `running` MUST include durable `state`. `succeeded` MUST include `dag_id`. `failed` MUST include `error`. `cancelled` MUST identify a successful cancel update and MAY omit durable execution output.

#### Scenario: First adapter call uses null state
- **WHEN** the runtime invokes an adapter for a new execution
- **THEN** the adapter envelope SHALL include `state = null`

#### Scenario: Cancel update includes cancellation fields
- **WHEN** the runtime invokes an adapter for a cancel update
- **THEN** the adapter envelope SHALL include `execution_status = "cancel-requested"`
- **AND** it SHALL include `cancel_requested_by`

#### Scenario: Later adapter state is ignored after first write
- **WHEN** the runtime invokes an adapter for an existing execution and the adapter returns `running` with a different `state`
- **THEN** the runtime SHALL continue using the existing stored `state` from `exec/state/<execution_id>.json`

#### Scenario: Cancel update may return cancelled
- **WHEN** an executor completes a cancel update successfully
- **THEN** the adapter result MAY use `status = "cancelled"`

#### Scenario: Pending is rejected
- **WHEN** an adapter returns `pending`
- **THEN** the runtime SHALL reject that result as invalid adapter output
