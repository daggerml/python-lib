## ADDED Requirements

### Requirement: Dml exposes a canonical config-var construction path
The system SHALL expose a `Dml` construction path that accepts the flattened canonical config-var keys used by the shared internal resolver.

#### Scenario: Canonical config-var dict feeds shared resolver directly
- **WHEN** a caller provides a flattened dictionary of canonical config vars
- **THEN** the `Dml` config-var factory forwards those keys into shared configuration resolution without requiring caller-side renaming

#### Scenario: Python constructor does not require dot-notation kwargs
- **WHEN** a caller constructs `Dml` through Python keyword arguments
- **THEN** the caller uses Python-friendly parameter names rather than canonical dot-notation keys
