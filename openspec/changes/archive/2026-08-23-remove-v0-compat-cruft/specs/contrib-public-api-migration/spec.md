## MODIFIED Requirements

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

## REMOVED Requirements

### Requirement: Contrib SHALL preserve adapter result semantics expected by runtime
**Reason**: The migration-era result normalization requirement preserves the retired `running` status and duplicates the current operation-specific adapter protocol.
**Migration**: None. Contrib adapters and executors return current `retry`, operation-specific success, or diagnostic failure responses directly.
