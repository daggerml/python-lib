## MODIFIED Requirements

### Requirement: Contrib worker DAGs SHALL be created from cache key and execution id
Contrib workers that need an execution-aware DAG SHALL first create or receive a `Dml` instance through the existing public session APIs, then create the worker DAG with the existing public DAG API using `cache_key` and `execution_id`. Contrib SHALL NOT require `temporary()` to accept execution identity and SHALL NOT require `api.new()` to accept `argv_ref`.

#### Scenario: Script worker creates execution-aware DAG
- **WHEN** the script worker receives `cache_key`, `execution_id`, and `remote.root`
- **THEN** it SHALL create a temporary Dml session using the existing public temporary-session API
- **AND** it SHALL create the worker DAG using the existing public DAG creation API with `cache_key` and `execution_id`
- **AND** it SHALL read call inputs through `dag.argv`

### Requirement: Contrib adapters SHALL conform to runtime operation contracts
Contrib adapter parsing SHALL accept `AdapterInvokeRequest` for invocation and `AdapterCancelRequest` for cancellation. Contrib SHALL derive worker DAG access for invocation from `cache_key`, `execution_id`, and `remote.root`. Nested invocation SHALL use `adapter_state`, and nested cancellation SHALL use the `argv_ref` supplied in the cancel request.

#### Scenario: Adapter receives invoke request
- **WHEN** a contrib adapter receives an `AdapterInvokeRequest` containing `cache_key`, `execution_id`, remote data, runnable data, and adapter state
- **THEN** it SHALL parse the request without cancellation-only fields

#### Scenario: Nested executor forwards operation payload
- **WHEN** a contrib executor delegates to a nested adapter through Docker, SSH, Batch, or another nested transport
- **THEN** it SHALL forward `adapter_state` and the applicable invoke or cancel request fields
- **AND** it SHALL preserve `argv_ref` when forwarding cancellation
