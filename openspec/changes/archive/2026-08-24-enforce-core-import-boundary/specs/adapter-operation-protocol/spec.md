## ADDED Requirements

### Requirement: Adapter response validation SHALL be available through the core facade
The `daggerml._core` package facade SHALL export the canonical adapter-response validation operation so adapter and executor implementations can validate invoke, cleanup, and cancel responses without importing a core implementation submodule. The facade operation SHALL preserve the response semantics defined by this capability.

#### Scenario: Extension validates an adapter response
- **WHEN** an adapter or executor validates an operation response
- **THEN** it can import the canonical validator directly from `daggerml._core`
- **AND** validation preserves the existing operation-specific status, state, delay, and error rules
