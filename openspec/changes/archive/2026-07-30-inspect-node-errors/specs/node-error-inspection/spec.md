## ADDED Requirements

### Requirement: Core node resolution preserves terminal error refs
Core node resolution SHALL return a pair of datum and error refs with exactly one populated, and it SHALL NOT hydrate or raise a stored terminal error while resolving a node.

#### Scenario: Resolve a successful function node
- **WHEN** a function node refers to a child DAG with a result
- **THEN** resolution returns the result datum ref and no error ref

#### Scenario: Resolve a failed function node
- **WHEN** a function node refers to a child DAG with an error ref
- **THEN** resolution returns no datum ref and that error ref without loading or raising the error object

### Requirement: DAG inspection materializes stored errors on request
The public DAG query surface SHALL return a hydrated stored `Error` when a requested node resolves to an error ref, and it SHALL provide a query to load a validated error ref directly.

#### Scenario: Inspect a failed node
- **WHEN** a caller invokes `dml.dag.get_node()` for a node resolving to an error ref
- **THEN** the query returns the hydrated raw `Error` represented by that ref

#### Scenario: Inspect an error ref directly
- **WHEN** a caller invokes `dml.dag.get_error()` with an `error:*` ref
- **THEN** the query returns the hydrated raw `Error`

#### Scenario: Reject a non-error ref
- **WHEN** a caller invokes `dml.dag.get_error()` with a ref outside the `error` namespace
- **THEN** the query raises a validation error

### Requirement: High-level failed node access raises contextual NodeError
The public API SHALL raise a transient `NodeError` when high-level node creation or value materialization reaches a stored error, and that exception SHALL retain the failed node ref and return its failed function-DAG context.

#### Scenario: Load a failed named node
- **WHEN** a caller accesses a named node that resolves to a stored error
- **THEN** the API raises `NodeError` with the requested node ref and the stored error fields

#### Scenario: Materialize a failed node
- **WHEN** a caller invokes `.value()` on a node that resolves to a stored error
- **THEN** the API raises `NodeError` with the node ref and the stored error fields

#### Scenario: Inspect failed function context
- **WHEN** a caller invokes `.context()` on a raised `NodeError`
- **THEN** it returns the function DAG that recorded the terminal error without materializing a result node

### Requirement: Errors cannot be consumed as execution inputs
Function invocation and cache-key computation SHALL require datum refs and SHALL raise a resolved stored error before creating an argv node, call node, cache key, or execution when any required node resolves to an error ref.

#### Scenario: Invoke with a failed node ref
- **WHEN** a function invocation input resolves to an error ref
- **THEN** the stored error is raised and no new function call or execution is created

### Requirement: Persisted errors are canonical base Error instances
The transaction storage boundary SHALL convert an `Error` subclass to a newly created exact base `Error` before persistence, retaining only the persisted error fields.

#### Scenario: Persist a transient NodeError
- **WHEN** a transient `NodeError` is supplied for persistence
- **THEN** the stored object is an exact base `Error` with matching message, origin, type, and stack
- **AND** it contains no transient node or DAG context fields
