## Purpose
Keep authoring helpers and tests free of type-checking no-op casts.

## Requirements

### Requirement: Contrib authoring helpers SHALL not use `cast(..., Any)` no-ops
The system SHALL preserve the current `api.dagclass`, `api.run`, and `api.funkify` behavior without using `cast(..., Any)` in their implementation.

#### Scenario: Dagclass decoration still works after cast removal
- **WHEN** `api.dagclass` decorates and runs a class that previously passed through `cast(..., Any)` sites
- **THEN** the existing decoration, compilation, and runtime behavior remain unchanged

#### Scenario: Funkify and dag staging still work after cast removal
- **WHEN** contrib runnable values are staged through the existing `api.funkify` and DAG execution flow
- **THEN** the same runtime results are produced without routing those values through `cast(..., Any)`

### Requirement: Tests SHALL not use `cast(..., Any)` no-ops
The test suite SHALL validate contrib and configuration behavior without using `cast(..., Any)` to pass values through unchanged.

#### Scenario: Invalid funkify input remains rejected without `Any` casts
- **WHEN** test coverage passes a concrete invalid input to `api.funkify`
- **THEN** the API still raises the existing invalid-input repository error
