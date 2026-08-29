## MODIFIED Requirements

### Requirement: Runtime SHALL maintain one mutable execution record per execution id
The runtime SHALL persist one mutable lifecycle object per execution id as `execution_record`, separate from caller-owned `launch_state`. `execution_record` SHALL include `execution_id`, `cache_key`, `lifecycle`, `updated_at`, `created_at`, `spawned_execution_ids`, `child_execution_ids`, and `cancellation_requested_by`, where `cancellation_requested_by` is `str | null`. `lifecycle` SHALL be one of `running`, `cancel-pending`, `cancelled`, `succeeded`, or `failed`. `spawned_execution_ids` SHALL be the deduped set of active direct child execution ids still in flight for cancellation traversal. `child_execution_ids` SHALL be the deduped set of completed direct child execution ids retained for lineage monitoring. `execution_record` updates SHALL use compare-and-swap with the latest known ETag. If a compare-and-swap update observes ETag drift, the runtime SHALL reread the record and SHALL raise cancellation interruption only when the reread lifecycle is already a `cancel-*` value; otherwise it SHALL continue from the latest valid reread state.

The same `execution_record` schema SHALL also be used for each live index id. For index-root records, the object path SHALL be `exec/state/<index_id>.json`, `execution_id` SHALL equal the `index_id`, `cache_key` SHALL equal the `index_id`, and both `spawned_execution_ids` and `child_execution_ids` SHALL track the deduped direct execution descendants started from that index according to their active vs terminal state.

`IndexOps.commit` SHALL always finalize the committing execution or root record as `lifecycle = "succeeded"`. A committed DAG `Error` value SHALL mean the execution successfully produced a DAG whose terminal result is an error. Runtime `failed` SHALL be reserved for execution-path failures that prevent successful DAG completion.

`IndexOps.commit` SHALL always update the committing execution or root record, and it SHALL publish a cache entry only when the committed DAG is runnable (`argv is not null`). Non-runnable DAG commits SHALL still finalize the execution/root record but SHALL NOT publish cache.

The `execution_record` schema SHALL be:

- `execution_id: str`
- `cache_key: str`
- `lifecycle: "running" | "cancel-pending" | "cancelled" | "succeeded" | "failed"`
- `updated_at: int`
- `created_at: int`
- `spawned_execution_ids: list[str]`
- `child_execution_ids: list[str]`
- `cancellation_requested_by: str | null`

#### Scenario: Index creation creates the initial execution record
- **WHEN** `IndexOps.create` initializes a new runtime root
- **THEN** it SHALL create an `execution_record` for that root before execution starts
- **AND** that record SHALL use `execution_id = index_id` and `cache_key = index_id`
- **AND** that record SHALL initialize `created_at` and `updated_at`
- **AND** that record SHALL initialize `spawned_execution_ids = []` and `child_execution_ids = []`

#### Scenario: Lifecycle record does not store resume state
- **WHEN** the runtime persists `execution_record` for execution `e0`
- **THEN** it SHALL NOT store adapter resume state in that object
- **AND** resume state SHALL instead live only in caller-owned `launch_state`

#### Scenario: CAS reread continues on non-cancellation drift
- **WHEN** a compare-and-swap update for `execution_record` observes an ETag conflict
- **AND** the reread lifecycle is `running`, `succeeded`, or `failed`
- **THEN** the runtime SHALL continue from the reread record instead of raising cancellation interruption

#### Scenario: CAS reread raises on cancellation lifecycle drift
- **WHEN** a compare-and-swap update for `execution_record` observes an ETag conflict
- **AND** the reread lifecycle is `cancel-pending` or `cancelled`
- **THEN** the runtime SHALL surface cancellation interruption rather than continuing normal execution updates

#### Scenario: Top-level cancellation stores user provenance
- **WHEN** a user cancels index `idx1`
- **THEN** `exec/state/idx1.json` SHALL store `cancellation_requested_by` as that user identity

#### Scenario: Nested cancellation stores execution provenance
- **WHEN** execution `e1` triggers `cancel(e2)` during nested cancellation
- **THEN** `exec/state/e2.json` SHALL store `cancellation_requested_by = "e1"`

#### Scenario: Cancellation terminal state is cancelled
- **WHEN** runtime cancellation completes for execution `e1`
- **THEN** `exec/state/e1.json` SHALL store `lifecycle = "cancelled"`

#### Scenario: Root record accumulates active spawned execution ids
- **WHEN** index `idx1` starts execution `e1`
- **THEN** the runtime SHALL update `exec/state/idx1.json` so that `spawned_execution_ids` contains `e1`
- **AND** `child_execution_ids` SHALL remain unchanged while `e1` is still active

#### Scenario: Commit of DAG error still records execution success
- **WHEN** an execution successfully commits a DAG whose terminal result is an `Error` value
- **THEN** the runtime SHALL record the committing execution record as `lifecycle = "succeeded"`
- **AND** it SHALL NOT treat that outcome as runtime `failed`

#### Scenario: Runtime failed is reserved for execution-path failure
- **WHEN** an adapter or execution path fails before a DAG result is successfully committed
- **THEN** the runtime MAY record the execution as `lifecycle = "failed"`
- **AND** that lifecycle SHALL describe execution failure rather than a committed DAG error result

#### Scenario: Non-runnable commit finalizes execution without cache publication
- **WHEN** `IndexOps.commit` finalizes an execution or root whose committed DAG has no `argv`
- **THEN** the runtime SHALL still update the committing execution or root record to `lifecycle = "succeeded"`
- **AND** it SHALL NOT publish a cache entry for that commit

#### Scenario: Caller record accumulates active spawned execution ids by caller execution id
- **WHEN** caller execution `e0` starts callee execution `e1`
- **THEN** the runtime SHALL read and compare-and-swap update `exec/state/e0.json`
- **AND** the updated `spawned_execution_ids` SHALL contain `e1`
- **AND** that update SHALL not require the caller cache key to be threaded separately

#### Scenario: Root caller uses index execution record
- **WHEN** top-level runtime root `idx1` starts callee execution `e1`
- **THEN** the runtime SHALL treat `idx1` as `caller_execution_id`
- **AND** it SHALL read and compare-and-swap update `exec/state/idx1.json`
- **AND** the updated `spawned_execution_ids` SHALL contain `e1`

#### Scenario: Terminal direct child moves from spawned to child lineage
- **WHEN** caller execution `e0` has active direct child `e1` in `spawned_execution_ids`
- **AND** `e1` reaches terminal lifecycle `succeeded`, `failed`, or `cancelled`
- **THEN** the runtime SHALL remove `e1` from `e0`'s `spawned_execution_ids`
- **AND** it SHALL add `e1` to `e0`'s `child_execution_ids`

#### Scenario: Spawned and child execution summaries remain disjoint
- **WHEN** the runtime persists an `execution_record`
- **THEN** no execution id SHALL appear in both `spawned_execution_ids` and `child_execution_ids` in the same record

## ADDED Requirements

### Requirement: Runtime SHALL expose descendant execution graphs from execution records
The runtime SHALL expose an execution-record-owned graph query that accepts root execution ids and returns only the reachable descendant closure from those roots. The payload SHALL have shape `{roots: list[str], nodes: dict[str, node_payload]}` where each `node_payload` contains `execution_id`, `cache_key`, `lifecycle`, `updated_at`, `created_at`, `cancel_requested_by`, `children`, and `spawned`. `children` SHALL be derived from `child_execution_ids`, and `spawned` SHALL be derived from `spawned_execution_ids`. The graph query SHALL read only execution-record objects and SHALL include each reachable execution at most once.

#### Scenario: Graph query returns active and completed descendants
- **WHEN** root execution `e0` has active descendant `e1` in `spawned_execution_ids` and completed descendant `e2` in `child_execution_ids`
- **THEN** the graph payload for root `e0` SHALL include nodes for `e0`, `e1`, and `e2`
- **AND** node `e0` SHALL report `spawned = ["e1"]` and `children = ["e2"]`

#### Scenario: Graph query traverses through completed intermediates
- **WHEN** root execution `e0` lists completed child `e1` in `child_execution_ids`
- **AND** execution `e1` lists child `e2` in either `spawned_execution_ids` or `child_execution_ids`
- **THEN** the graph payload rooted at `e0` SHALL include `e2`

#### Scenario: Graph query excludes unrelated executions
- **WHEN** execution record `e9` exists but is not reachable from the requested roots through `spawned_execution_ids` or `child_execution_ids`
- **THEN** the graph payload SHALL NOT include node `e9`

#### Scenario: Graph query uses execution records only
- **WHEN** the runtime assembles the descendant graph payload
- **THEN** it SHALL NOT require DAG objects, launch-state objects, edge files, active refs, or cache refs to shape the response
