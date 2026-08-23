## Purpose
Define contrib use of public worker APIs and runtime adapter operation contracts.

## Requirements

### Requirement: Contrib worker DAGs SHALL be created from cache key and execution id
Contrib workers that need an execution-aware DAG SHALL first create or receive a `Dml` instance through the existing public session APIs, then create the worker DAG with the existing public DAG API using `cache_key` and `execution_id`. Contrib SHALL NOT require `temporary()` to accept execution identity and SHALL NOT require `api.new()` to accept `argv_ref`.

#### Scenario: Script worker creates execution-aware DAG
- **WHEN** the script worker receives `cache_key`, `execution_id`, and `remote.root`
- **THEN** it SHALL create a temporary Dml session using the existing public temporary-session API
- **AND** it SHALL create the worker DAG using the existing public DAG creation API with `cache_key` and `execution_id`
- **AND** it SHALL read call inputs through `dag.argv`

### Requirement: Contrib SHALL use existing public APIs where sufficient
Contrib modules SHALL prefer existing public `daggerml.api` or package-root exports for Dml sessions, DAG wrappers, node wrappers, public value wrappers, DAG creation, loading, temporary sessions, and default-runtime access. Direct private `_core` imports SHALL remain only where no existing public import or API surface covers the required behavior.

#### Scenario: Public value wrapper is sufficient
- **WHEN** contrib code needs public value wrappers such as `Runnable`, `Uri`, `Ref`, or `Error`
- **THEN** it SHALL import them from the public API or package root instead of private `_core` modules when those public exports are available

#### Scenario: Private runtime behavior is not needed
- **WHEN** contrib code creates, loads, mutates, calls, or commits DAG values
- **THEN** it SHALL use existing public DAG/session APIs rather than direct lower-level runtime operations unless there is no public equivalent

### Requirement: Contrib adapters SHALL conform to runtime operation contracts
Contrib adapter parsing SHALL accept the current exact invoke, cleanup, and cancel request contracts. Contrib SHALL derive worker DAG access for invocation from `cache_key`, `execution_id`, and `remote.root`. Nested invocation SHALL use `adapter_state`, and cancellation SHALL preserve `argv_ref` under that exact name across the adapter request, executor dispatch, executor plugin signature, built-in executor, and nested adapter forwarding.

#### Scenario: Adapter receives invoke request
- **WHEN** a contrib adapter receives a current invoke request containing execution, remote, runnable, and adapter-state fields
- **THEN** it parses the exact request without cancellation-only fields

#### Scenario: Executor receives cancellation argv ref
- **WHEN** a contrib adapter dispatches cancellation
- **THEN** the executor plugin receives keyword `argv_ref` with the request's unchanged value
- **AND** it does not receive `argv_ptr`

#### Scenario: Nested executor forwards operation payload
- **WHEN** a contrib executor delegates to a nested adapter through Docker, SSH, Batch, or another nested transport
- **THEN** it forwards the exact applicable current operation fields
- **AND** it preserves `argv_ref` when forwarding cancellation
