## MODIFIED Requirements

### Requirement: Public committed collection reads SHALL expose `Projection` wrappers for interrogation
The public `daggerml.api` collection-wrapper surface SHALL allow committed dict/list reads to return `Projection` wrappers for ex-post interrogation without mutating repository state. Public staging and execution entrypoints SHALL accept those wrappers through ordinary codec normalization when they originate from the active target DAG's `Dml` instance.

#### Scenario: Committed collection read returns projection wrapper
- **WHEN** a caller reads a projected subvalue from a committed collection-valued `Node`
- **THEN** the public API may return a `Projection` wrapper instead of a real `Node` when the selected subvalue has no standalone persisted node identity

#### Scenario: Projection traversal remains read-only
- **WHEN** a caller creates or extends a public `Projection` wrapper
- **THEN** the wrapper provides read-only interrogation without staging nodes or mutating the committed source DAG

#### Scenario: Projection participates in codec-driven public inputs
- **WHEN** a caller passes a same-`Dml` projection directly or inside another normalized value to a public staging or execution entrypoint
- **THEN** the entrypoint accepts it through the shared codec normalization path
- **AND** the entrypoint does not require projection-specific dispatch logic
