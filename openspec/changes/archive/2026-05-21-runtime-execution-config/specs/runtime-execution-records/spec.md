## MODIFIED Requirements

### Requirement: Runtime SHALL maintain one mutable execution record per execution id
The runtime SHALL persist one mutable lifecycle object per execution id as `execution_record`, separate from caller-owned `launch_state`. `execution_record` SHALL include `execution_id`, `cache_key`, `lifecycle`, `updated_at`, `spawned_execution_ids`, and `cancellation_requested_by`, where `cancellation_requested_by` is `str | null`. `lifecycle` SHALL be one of `running`, `cancel-pending`, `cancel-detached`, `succeeded`, or `failed`. `spawned_execution_ids` SHALL be the deduped set of child execution ids started by that execution for cancellation traversal. `execution_record` updates SHALL use compare-and-swap with the latest known ETag.

`IndexOps.commit` SHALL always finalize the committing execution or root record as `lifecycle = "succeeded"`. A committed DAG `Error` value SHALL mean the execution successfully produced a DAG whose terminal result is an error. Runtime `failed` SHALL be reserved for execution-path failures that prevent successful DAG completion.

`IndexOps.commit` SHALL always update the committing execution or root record, and it SHALL publish a cache entry only when the committed DAG is runnable (`argv is not null`). Non-runnable DAG commits SHALL still finalize the execution/root record but SHALL NOT publish cache.

#### Scenario: Runnable DAG publication uses explicit execution identity
- **WHEN** an execution-aware worker commits a runnable DAG result
- **THEN** the runtime publishes the cache entry using the explicit `execution_id` provided through the runtime execution-aware call path
- **AND** it does not discover that identity through a process-local execution context object

#### Scenario: Non-runnable commit finalizes execution without cache publication
- **WHEN** `IndexOps.commit` finalizes an execution or root whose committed DAG has no `argv`
- **THEN** the runtime SHALL still update the committing execution or root record to `lifecycle = "succeeded"`
- **AND** it SHALL NOT publish a cache entry for that commit

#### Scenario: Commit of DAG error still records execution success
- **WHEN** an execution successfully commits a DAG whose terminal result is an `Error` value
- **THEN** the runtime SHALL record the committing execution record as `lifecycle = "succeeded"`
- **AND** it SHALL NOT treat that outcome as runtime `failed`

#### Scenario: Runtime failed is reserved for execution-path failure
- **WHEN** an adapter or execution path fails before a DAG result is successfully committed
- **THEN** the runtime MAY record the execution as `lifecycle = "failed"`
- **AND** that lifecycle SHALL describe execution failure rather than a committed DAG error result

#### Scenario: Caller record accumulates spawned execution ids by caller execution id
- **WHEN** caller execution `e0` starts callee execution `e1`
- **THEN** the runtime SHALL read and compare-and-swap update `exec/state/e0.json`
- **AND** the updated `spawned_execution_ids` SHALL contain `e1`
- **AND** that update SHALL not require the caller cache key to be threaded separately

#### Scenario: Root caller uses index execution record
- **WHEN** top-level runtime root `idx1` starts callee execution `e1`
- **THEN** the runtime SHALL treat `idx1` as `caller_execution_id`
- **AND** it SHALL read and compare-and-swap update `exec/state/idx1.json`
- **AND** the updated `spawned_execution_ids` SHALL contain `e1`

### Requirement: Commit lifecycle distinction SHALL be documented in code and spec
The runtime SHALL document at the `IndexOps.commit` lifecycle update site that committing an `Error` value is still a successful execution, and that runtime `failed` is reserved for execution-path failures that prevent successful DAG completion.

#### Scenario: Commit lifecycle distinction is documented at implementation site
- **WHEN** maintainers inspect the execution-record lifecycle update in `IndexOps.commit`
- **THEN** the code includes a comment explaining why committed `Error` values still map to `lifecycle = "succeeded"`
- **AND** the comment distinguishes DAG error results from runtime execution failures
