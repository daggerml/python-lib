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
Contrib modules SHALL use public `daggerml.api`, package-root, or `daggerml._core` facade exports for Dml sessions, DAG wrappers, node wrappers, public value wrappers, adapter protocol contracts, DAG creation, loading, temporary sessions, default-runtime access, and runtime inspection. Contrib modules MUST NOT import `daggerml._core` implementation submodules. When required behavior lacks a public equivalent, the owning boundary SHALL expose a deliberate public operation or facade export instead of allowing contrib to bypass the boundary.

#### Scenario: Public value wrapper is sufficient
- **WHEN** contrib code needs public value wrappers such as `Runnable`, `Uri`, `Ref`, or `Error`
- **THEN** it SHALL import them from the public API or package root instead of private `_core` modules

#### Scenario: Runtime inspection is required
- **WHEN** contrib code needs a stored execution record or published result ref
- **THEN** it SHALL inspect the execution through the public `Dml.runtime` API
- **AND** it SHALL NOT instantiate or import the private execution-state implementation

#### Scenario: Required core contract lacks a facade export
- **WHEN** contrib requires a core-owned cross-boundary contract that is not publicly exposed
- **THEN** the contract SHALL be deliberately exported through the owning public facade before contrib uses it
- **AND** contrib SHALL NOT import its defining `_core` submodule

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
