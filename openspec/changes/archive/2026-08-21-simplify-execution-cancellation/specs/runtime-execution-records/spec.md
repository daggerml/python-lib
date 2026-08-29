## MODIFIED Requirements

### Requirement: Runtime SHALL maintain one mutable execution record per execution id
The runtime SHALL persist `execution/<execution_id>` with fields `execution_id`, `cache_key`, `lifecycle`, `created_at`, `updated_at`, `lock`, `adapter_state`, `argv_ref`, `result_ref`, `spawned_execution_ids`, `child_execution_ids`, `cancelation`, and `invalidation`. `execution_id` SHALL be nonempty; `cache_key` null or nonempty; timestamps non-boolean nonnegative integers with `updated_at >= created_at`; and `lock` null or exact `{owner: nonempty str, ttl: positive finite non-boolean number}`. `adapter_state` SHALL be an object or null. `argv_ref` and `result_ref` SHALL be syntactically typed `node-argv` and `dag` ref strings respectively, or null; validation SHALL NOT check whether either ref exists in storage. Lineage lists SHALL contain unique nonempty execution IDs and be disjoint. `cancelation` and `invalidation` SHALL each be null or exact objects containing nonempty `requested_by` and a non-boolean nonnegative integer `requested_at`. Lifecycle SHALL be one of `pending`, `running`, `succeeded`, `failed`, `cancel-pending`, or `canceled`; `cancel-requested` and `cancel-ready` SHALL NOT be accepted. Lifecycle and lineage semantics SHALL remain execution-owned, and every mutation SHALL require the embedded lock owner and CAS.

#### Scenario: Fresh child record is complete and locked
- **WHEN** the runtime reserves execution `e1` for cache key `ck1`
- **THEN** it creates `execution/e1` with lifecycle `pending`, a fresh owner lock, its argv ref, null adapter and result state, empty lineage, and null control state

#### Scenario: Result is stored in the same record
- **WHEN** execution `e1` completes with DAG `dag:d1`
- **THEN** its lock owner conditionally stores `result_ref = "dag:d1"` and a terminal lifecycle in `execution/e1`

#### Scenario: Every mutation requires ownership
- **WHEN** a caller attempts to change lifecycle, adapter state, refs, lineage, cancelation, or invalidation
- **THEN** it SHALL hold the matching execution lock owner

#### Scenario: Cancel-pending is the only cancellation intermediate
- **WHEN** an execution record is validated or written
- **THEN** `cancel-pending` SHALL be accepted as the only nonterminal cancellation lifecycle
- **AND** `cancel-requested` and `cancel-ready` SHALL be rejected

### Requirement: Adapter operations SHALL follow the runtime-owned execution contract
The runtime SHALL use `AdapterInvokeRequest` / `AdapterInvokeResponse` for invocation and `AdapterCancelRequest` / `AdapterCancelResponse` for cancellation. Invoke requests SHALL carry invocation data and current `adapter_state` without cancellation-only fields. Cancel requests SHALL carry `argv_ref` from the unified execution record. Cancel-path adapter responses SHALL NOT control runtime lifecycle persistence. After Phase 1 has selected the complete cancellation set, Phase 2 SHALL issue the applicable cancel operation for each selected adapter-backed execution itself rather than waiting for a child-readiness lifecycle.

#### Scenario: First adapter call uses null adapter state
- **WHEN** the runtime invokes an adapter for a new execution
- **THEN** the `AdapterInvokeRequest` SHALL include null `adapter_state`

#### Scenario: Cancel update uses execution-owned target
- **WHEN** the runtime invokes an adapter for a selected cancellation
- **THEN** the runtime SHALL send an `AdapterCancelRequest` with the target execution ID
- **AND** it SHALL include `argv_ref` from that execution's record

#### Scenario: Runtime ignores cancel return for terminal lifecycle write
- **WHEN** an adapter returns from a cancel update
- **THEN** the runtime SHALL NOT require a specific adapter success token before writing `lifecycle = "canceled"`

#### Scenario: Every selected adapter-backed execution receives its own cancel update
- **WHEN** Phase 1 selects a parent and one or more spawned adapter-backed executions
- **THEN** Phase 2 SHALL process each selected execution's applicable cancel adapter
- **AND** it SHALL NOT require recursive adapter cancellation to discover the selected set

#### Scenario: Cancellation requester is stable across the selected set
- **WHEN** root cancellation selects nested executions
- **THEN** each newly selected execution's `cancelation.requested_by` SHALL identify the requester of that cancellation operation
- **AND** a resumed drive SHALL preserve already-persisted requester metadata

#### Scenario: Pending is rejected
- **WHEN** an adapter returns `pending`
- **THEN** the runtime SHALL reject that result as invalid adapter output

### Requirement: Runtime SHALL durably register a child before adapter invocation
For an adapter-backed child execution, the runtime SHALL publish the caller edge and append the child execution ID to the caller's `spawned_execution_ids` through successful coordinated updates before invoking the adapter. Caller registration SHALL serialize with cancellation selection for the callee and SHALL verify that the callee lifecycle still permits invocation. The runtime SHALL retry CAS conflicts with bounded backoff. If registration observes `cancel-pending` or `canceled`, or exhausts its retry budget, it SHALL fail the launch, remove any incomplete caller edge it owns, and SHALL NOT invoke the adapter.

#### Scenario: Cancellation selection wins child-registration contention
- **WHEN** cancellation persists `cancel-pending` for callee `e1` before caller registration completes
- **THEN** registration of `e1` SHALL fail
- **AND** the runtime SHALL remove its incomplete caller edge
- **AND** it SHALL NOT invoke `e1`'s adapter

#### Scenario: Child registration wins cancellation contention
- **WHEN** registration completes a valid caller edge for `e1` before cancellation evaluates caller references
- **THEN** cancellation planning SHALL observe that valid caller reference
- **AND** it SHALL leave `e1` active

#### Scenario: Child registration exhausts retries
- **WHEN** registration cannot complete after the bounded CAS retry budget
- **THEN** the runtime SHALL raise a coordination failure
- **AND** it SHALL NOT invoke the adapter

## ADDED Requirements

### Requirement: Adapter cancellation SHALL advance directly from cancel-pending to canceled
For every adapter-backed execution in the Phase 1 cancellation set, Phase 2 SHALL build an `AdapterCancelRequest` from that execution's record, invoke the adapter synchronously, and compare-and-swap lifecycle from `cancel-pending` directly to `canceled`. If adapter invocation or lifecycle persistence is interrupted, the execution SHALL remain recoverable from `cancel-pending`, and repeated cancellation SHALL be safe.

#### Scenario: Adapter cancellation completes
- **WHEN** the applicable cancel adapter returns for a `cancel-pending` execution
- **THEN** the runtime SHALL compare-and-swap that execution directly to `canceled`

#### Scenario: Cancellation resumes after interruption
- **WHEN** adapter work is interrupted before `canceled` is persisted
- **THEN** the execution SHALL remain `cancel-pending`
- **AND** a later drive SHALL be able to repeat the idempotent cancel operation

## REMOVED Requirements

### Requirement: Adapter cancel dispatch SHALL target direct children that are cancel-ready
**Reason**: The cancellation planner now selects all eligible executions before adapter work, eliminating direct-child readiness handoff.

**Migration**: Invoke the applicable cancel adapter for each execution in the persisted `cancel-pending` set and transition it directly to `canceled`.
