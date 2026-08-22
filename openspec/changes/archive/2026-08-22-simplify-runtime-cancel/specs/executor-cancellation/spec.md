## ADDED Requirements

### Requirement: Successful executor cancellation SHALL gate terminal cancellation
Executors SHALL return `cancelled` only after their synchronous cancellation step succeeds. The runtime SHALL remain responsible for lifecycle persistence and SHALL transition `cancel-pending` to `canceled` only for that successful outcome.

#### Scenario: Successful executor cancellation becomes terminal
- **WHEN** an executor successfully completes cancellation and returns `cancelled`
- **THEN** the runtime SHALL persist the execution as `canceled`

#### Scenario: Unsuccessful executor cancellation remains pending
- **WHEN** executor cancellation returns another outcome or raises
- **THEN** the runtime SHALL leave the execution `cancel-pending` for bounded retry

## REMOVED Requirements

### Requirement: Cancel-path return values SHALL remain advisory only
**Reason**: Cancellation success now gates the terminal lifecycle transition.

**Migration**: Executors return `cancelled` only after successful teardown and remain idempotent across retries.
