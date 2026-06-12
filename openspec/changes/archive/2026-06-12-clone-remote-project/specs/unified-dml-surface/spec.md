## ADDED Requirements

### Requirement: Shared `Dml` class exposes clone bootstrap
The shared `Dml` surface SHALL expose `clone` as a classmethod bootstrap workflow alongside `init`.

#### Scenario: Caller discovers clone on shared Dml surface
- **WHEN** a caller inspects the shared `Dml` class
- **THEN** clone bootstrap is available as `Dml.clone(...)` rather than as an instance method or external helper
