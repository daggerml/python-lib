## MODIFIED Requirements

### Requirement: Shared `Dml` exact DB object contracts use `Ref`
The shared `Dml` surface SHALL require `Ref` objects for exact DB-backed object inputs and for runtime or execution identity inputs, and SHALL return `Ref` objects as canonical runtime and DB identity. Revision selectors, DAG and node names, branch and tag names, endpoint roots, dependency names, cache keys, and lower-level execution-state IDs SHALL remain strings. `Dml.runtime` SHALL convert supplied runtime refs to string execution IDs only when delegating below the shared surface and SHALL NOT convert caller strings into refs.

#### Scenario: Exact DAG access requires `Ref`
- **WHEN** a caller invokes a `Dml` method whose contract is to dereference an exact DAG object
- **THEN** the method requires a `Ref`
- **AND** it does not accept a plain `"dag:..."` string as a substitute

#### Scenario: Exact node access requires `Ref`
- **WHEN** a caller invokes a `Dml` method whose contract is to dereference an exact node object
- **THEN** the method requires a `Ref`
- **AND** it does not accept a plain `"node:..."` string as a substitute

#### Scenario: Runtime execution identity requires `Ref`
- **WHEN** a caller supplies an execution identity to `Dml.runtime.create`, `read_launch_state`, `read_execution_record`, `describe_graph`, or `cancel`
- **THEN** the shared `Dml` method requires an `index` or otherwise method-supported runtime `Ref`
- **AND** it does not accept a bare execution ID or plain `"index:..."` string as a substitute

#### Scenario: Non-identity selectors remain strings
- **WHEN** a caller provides revision text, a symbolic object name, an endpoint root, a dependency name, or a cache key
- **THEN** the shared `Dml` surface continues to accept that value as a string

#### Scenario: Lower-level execution state remains string addressed
- **WHEN** a `Dml.runtime` method delegates a validated runtime ref to `IndexOps` or `ExecutionState`
- **THEN** it passes the ref's exact id as the lower-level execution ID string
- **AND** the lower-level persisted and protocol representation does not change

#### Scenario: DB-backed payloads use ref identity
- **WHEN** a shared `Dml` payload includes the identity of a commit, DAG, node, runtime, or other DB-backed object
- **THEN** that identity is represented by `Ref`
- **AND** the payload does not duplicate the same DB identity as a separate raw id string

### Requirement: Shared `Dml` runtime namespace SHALL expose launch-state inspection
The shared `Dml` runtime namespace SHALL expose `read_launch_state(execution: Ref) -> dict | None`, require an `index` or supported runtime ref, delegate `execution.id()` to the execution-state launch reader, and return its result without reshaping it. Launch-state content and absence semantics are owned by `runtime-execution-records`.

#### Scenario: Runtime namespace returns persisted resume state
- **WHEN** a caller invokes `dml.runtime.read_launch_state(Ref("index:e1"))` and launch state exists for execution `e1`
- **THEN** the method delegates execution ID `e1`
- **AND** it returns the persisted resume-state JSON object

#### Scenario: Runtime namespace preserves an absent launch state
- **WHEN** a caller invokes `dml.runtime.read_launch_state(Ref("index:missing"))` and no launch state exists
- **THEN** the method returns `None`

#### Scenario: Launch-state inspection rejects string identity
- **WHEN** a Python caller invokes `dml.runtime.read_launch_state("index:e1")` or `dml.runtime.read_launch_state("e1")`
- **THEN** the method fails before delegating to execution state

### Requirement: Shared `Dml` runtime namespace SHALL expose direct execution-record inspection
The shared `Dml` runtime namespace SHALL expose `read_execution_record(execution: Ref)` for direct execution-state inspection. The runtime namespace SHALL validate the runtime ref and delegate its id as an execution-id string. When the read succeeds, the method SHALL return the raw execution record typed dict without reshaping or enrichment.

#### Scenario: Runtime namespace reads an execution record from a runtime ref
- **WHEN** a caller invokes `dml.runtime.read_execution_record(Ref("index:exec-2"))`
- **THEN** the runtime namespace SHALL delegate using execution ID `exec-2`
- **AND** it SHALL return the stored execution record unchanged

#### Scenario: Runtime namespace rejects an execution id string
- **WHEN** a Python caller invokes `dml.runtime.read_execution_record("exec-2")` or `dml.runtime.read_execution_record("index:exec-2")`
- **THEN** the runtime namespace SHALL fail before reading execution state

#### Scenario: Runtime namespace preserves underlying read failures
- **WHEN** a caller invokes `dml.runtime.read_execution_record(Ref("index:missing"))`
- **AND** no execution record exists for `missing`
- **THEN** the runtime namespace SHALL surface the same missing-record failure from the underlying execution-state reader

### Requirement: Shared `Dml` runtime namespace SHALL normalize ref roots for execution graph inspection
The shared `Dml` runtime namespace SHALL expose `describe_graph(*roots: Ref, visual: bool = False)` for execution-lineage inspection. If the caller provides no roots, the runtime namespace SHALL use all currently open local runtime refs as roots. Before delegating to execution-state graph extraction, the runtime namespace SHALL validate the selected refs and extract their execution-id strings. When `visual` is `False`, the method SHALL return the extracted `ExecutionGraph`. When `visual` is `True`, the method SHALL render a human-friendly execution graph view and return `None`.

#### Scenario: Explicit ref roots are normalized and delegated
- **WHEN** a caller invokes `dml.runtime.describe_graph(Ref("index:idx1"), Ref("index:exec-2"))`
- **THEN** the runtime namespace SHALL delegate graph extraction with root IDs `idx1` and `exec-2`

#### Scenario: String graph roots are rejected
- **WHEN** a Python caller invokes `dml.runtime.describe_graph("idx1")` or `dml.runtime.describe_graph("index:idx1")`
- **THEN** the runtime namespace SHALL fail before graph extraction

#### Scenario: Empty input defaults to open local indexes
- **WHEN** a caller invokes `dml.runtime.describe_graph()` with no explicit roots
- **THEN** the runtime namespace SHALL read the currently open local runtime refs
- **AND** it SHALL use those ref ids as the root execution ids for graph extraction

#### Scenario: Visual mode renders instead of returning the raw graph
- **WHEN** a caller invokes `dml.runtime.describe_graph(Ref("index:idx1"), visual=True)`
- **THEN** the runtime namespace SHALL fetch the same execution graph data it would use for raw inspection
- **AND** it SHALL render a human-friendly execution graph view
- **AND** it SHALL return `None`

### Requirement: Shared `Dml` exposes runtime cancel with explicit mode selection
The shared `Dml` runtime namespace SHALL expose cancellation as `dml.runtime.cancel(index: Ref, mode="full")`. It SHALL validate the runtime ref, delegate its id to execution state, and preserve the supplied `Ref` as the requested identity in the returned summary. `mode` SHALL accept `"full"` and `"drive"`.

- `mode = "full"` SHALL run the full root-facing cancellation workflow.
- `mode = "drive"` SHALL run only the cancellation driver needed by an already-canceling execution.

#### Scenario: Runtime namespace exposes full cancellation by default
- **WHEN** a caller invokes `dml.runtime.cancel(Ref("index:idx1"))` without an explicit mode
- **THEN** the runtime namespace SHALL use `mode = "full"`
- **AND** it SHALL delegate execution ID `idx1`

#### Scenario: Runtime namespace exposes drive mode for internal cancellation progress
- **WHEN** a caller invokes `dml.runtime.cancel(Ref("index:e1"), mode="drive")`
- **THEN** the runtime namespace SHALL expose the driver-only cancellation behavior for execution `e1`

#### Scenario: Runtime cancellation rejects string identity
- **WHEN** a Python caller invokes `dml.runtime.cancel("e1")` or `dml.runtime.cancel("index:e1")`
- **THEN** the runtime namespace SHALL fail before cancellation state is changed

### Requirement: Direct user cancellation SHALL use configured user identity
When `dml.runtime.cancel(index)` is invoked with a runtime `Ref` and without an active runtime execution context, the workflow SHALL still proceed as an out-of-band cancellation operation. In that case, the runtime SHALL record `cancellation_requested_by` from the configured user identity.

#### Scenario: User-triggered cancel records configured user without active execution
- **WHEN** a user directly invokes `dml.runtime.cancel(Ref("index:idx1"))`
- **AND** there is no active caller execution context
- **THEN** the runtime SHALL set `cancellation_requested_by` to `config.user`

#### Scenario: Missing configured user still fails cancel
- **WHEN** a user invokes `dml.runtime.cancel(Ref("index:idx1"))`
- **AND** no configured user identity is available
- **THEN** the runtime SHALL fail the request rather than persisting an empty cancellation requester

### Requirement: Runtime cancellation SHALL be out-of-band control-plane behavior
`dml.runtime.cancel(index: Ref)` SHALL operate as an out-of-band control-plane workflow rather than as a continuation of a running execution. The workflow SHALL freeze the target index, remove caller-owned live edges, orphan eligible callees, and request detached cancellation without requiring an active caller execution context.

#### Scenario: Direct cancel freezes index before cancellation traversal
- **WHEN** a user invokes `dml.runtime.cancel(Ref("index:idx1"))`
- **THEN** the runtime SHALL freeze the index before removing live caller edges or requesting callee cancellation

## ADDED Requirements

### Requirement: Shared `Dml` runtime creation SHALL accept optional execution identity as `Ref`
The shared `Dml` runtime namespace SHALL expose execution-aware creation as `create(cache_key: str | None = None, execution: Ref | None = None) -> Ref`. `cache_key` and `execution` SHALL either both be supplied or both be omitted. When supplied, `execution` SHALL be an `index` ref and `Dml` SHALL delegate its id as the lower-level execution ID. The method SHALL NOT accept an execution-id string or provide a compatibility alias for the former string parameter.

#### Scenario: Ordinary creation generates runtime identity
- **WHEN** a caller invokes `dml.runtime.create()` without cache or execution identity
- **THEN** the method creates a runtime with a generated identity
- **AND** it returns the runtime `Ref`

#### Scenario: Execution-aware creation extracts ref id
- **WHEN** a caller invokes `dml.runtime.create(cache_key="ck1", execution=Ref("index:e1"))`
- **THEN** the runtime namespace delegates `cache_key="ck1"` and `execution_id="e1"` to index creation
- **AND** it returns the created runtime `Ref`

#### Scenario: Execution-aware creation rejects string identity
- **WHEN** a Python caller supplies `execution="e1"` or `execution="index:e1"`
- **THEN** runtime creation fails before index activation

#### Scenario: Execution-aware creation preserves paired inputs
- **WHEN** exactly one of `cache_key` and `execution` is supplied
- **THEN** runtime creation fails without creating local index state
