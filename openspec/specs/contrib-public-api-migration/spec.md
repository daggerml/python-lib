## Purpose
Define contrib use of public worker APIs and runtime adapter operation contracts.

## Requirements

### Requirement: Contrib implementation SHALL remain contrib-scoped
Implementation for this change SHALL modify only contrib-owned code paths. Application-code changes SHALL be limited to `src/daggerml/contrib/**`. Test or human-documentation updates, if required, SHALL be limited to contrib-scoped tests or contrib-scoped documentation. The implementation SHALL NOT modify `src/daggerml/api.py`, `src/daggerml/_core/dml.py`, any other `src/daggerml/_core/**` file, package-root exports, CLI code, storage code, or unrelated non-contrib modules.

#### Scenario: Core change is discovered during implementation
- **WHEN** implementation appears to require a change outside contrib-owned paths
- **THEN** implementation SHALL stop and treat that need as a proposal/design blocker
- **AND** the implementer SHALL NOT modify the non-contrib file as part of this change

#### Scenario: Implementation edits are reviewed
- **WHEN** the implementation diff is reviewed
- **THEN** all application-code edits SHALL be under `src/daggerml/contrib/**`
- **AND** no edits SHALL appear in `src/daggerml/api.py`, `src/daggerml/_core/**`, or unrelated source files

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
Contrib adapter parsing SHALL accept `AdapterInvokeRequest` for invocation and `AdapterCancelRequest` for cancellation. Contrib SHALL derive worker DAG access for invocation from `cache_key`, `execution_id`, and `remote.root`. Nested invocation SHALL use `adapter_state`, and nested cancellation SHALL use the `argv_ref` supplied in the cancel request.

#### Scenario: Adapter receives invoke request
- **WHEN** a contrib adapter receives an `AdapterInvokeRequest` containing `cache_key`, `execution_id`, remote data, runnable data, and adapter state
- **THEN** it SHALL parse the request without cancellation-only fields

#### Scenario: Nested executor forwards operation payload
- **WHEN** a contrib executor delegates to a nested adapter through Docker, SSH, Batch, or another nested transport
- **THEN** it SHALL forward `adapter_state` and the applicable invoke or cancel request fields
- **AND** it SHALL preserve `argv_ref` when forwarding cancellation

### Requirement: Contrib SHALL preserve adapter result semantics expected by runtime
Contrib adapters and executors SHALL return terminal and non-terminal execution results in the shape expected by the current runtime caller. Any status/lifecycle normalization required for contrib executors SHALL be handled within contrib-owned adapter code.

#### Scenario: Executor reports running
- **WHEN** a contrib executor reports that work is still in progress
- **THEN** the contrib adapter boundary SHALL return a runtime-accepted non-terminal result with durable resume state when required

#### Scenario: Executor reports success
- **WHEN** a contrib executor reports a successful DAG result
- **THEN** the contrib adapter boundary SHALL return a runtime-accepted terminal success result carrying the produced DAG identity

#### Scenario: Executor reports failure
- **WHEN** a contrib executor reports execution-path failure
- **THEN** the contrib adapter boundary SHALL return a runtime-accepted terminal failure result carrying an error message
