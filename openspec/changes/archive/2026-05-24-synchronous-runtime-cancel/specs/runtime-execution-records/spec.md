## MODIFIED Requirements

### Requirement: Runtime SHALL maintain one mutable execution record per execution id
The runtime SHALL persist one mutable lifecycle object per execution id as `execution_record`, separate from caller-owned `launch_state`. `execution_record` SHALL include `execution_id`, `cache_key`, `lifecycle`, `updated_at`, `spawned_execution_ids`, and `cancellation_requested_by`, where `cancellation_requested_by` is `str | null`. `lifecycle` SHALL be one of `running`, `cancel-pending`, `cancelled`, `succeeded`, or `failed`.

`cancellation_requested_by` SHALL record the immediate requester for the current cancel call and MAY contain either a user identity or an execution id.

#### Scenario: Top-level cancellation stores user provenance
- **WHEN** a user cancels index `idx1`
- **THEN** `exec/state/idx1.json` SHALL store `cancellation_requested_by` as that user identity

#### Scenario: Nested cancellation stores execution provenance
- **WHEN** execution `e1` triggers `cancel(e2)` during nested cancellation
- **THEN** `exec/state/e2.json` SHALL store `cancellation_requested_by = "e1"`

#### Scenario: Cancellation terminal state is cancelled
- **WHEN** runtime cancellation completes for execution `e1`
- **THEN** `exec/state/e1.json` SHALL store `lifecycle = "cancelled"`

### Requirement: Adapter envelope and result schema SHALL follow the runtime-owned execution contract
The adapter envelope SHALL continue to include `argv_ptr`, `cache_key`, `execution_id`, `remote`, `runnable`, `state`, `execution_status`, and `cancel_requested_by`. For cancellation, the runtime SHALL send `execution_status = "cancel-pending"` only to adapter chains responsible for direct child executions.

Cancel-path adapter returns SHALL NOT control runtime lifecycle persistence. The runtime SHALL ignore those cancel return values when deciding whether to write `cancelled`.

#### Scenario: Cancel update includes pending lifecycle and requester provenance
- **WHEN** the runtime invokes an adapter for a cancel update
- **THEN** the adapter envelope SHALL include `execution_status = "cancel-pending"`
- **AND** it SHALL include `cancel_requested_by`

#### Scenario: Runtime ignores cancel return for terminal lifecycle write
- **WHEN** an adapter returns from a cancel update
- **THEN** the runtime SHALL NOT require a `cancelled` adapter result before writing `lifecycle = "cancelled"`

#### Scenario: Parent cancellation targets child adapter chains only
- **WHEN** `cancel(e1)` fans out over direct children
- **THEN** the runtime SHALL send cancel updates only to adapter chains responsible for those child executions
- **AND** it SHALL NOT separately send a cancel update for `e1` itself as part of that parent cancel flow

#### Scenario: Immediate parent is recorded for nested cancellation
- **WHEN** root cancellation of `idx1` leads to nested `cancel(e2)` through `e1`
- **THEN** `exec/state/e2.json` SHALL record `cancellation_requested_by = "e1"`
- **AND** it SHALL NOT replace that field with `idx1` solely because `idx1` started the overall cancellation tree
