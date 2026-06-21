## MODIFIED Requirements

### Requirement: Executors SHALL handle runtime cancel invocation as a synchronous cancellation step
When the runtime invokes an executor through the cancellation path for a direct child execution, the executor SHALL treat that invocation as synchronous cancellation work. The executor cancel contract SHALL remain separate from execution-record-only lifecycle states such as `cancel-ready`.

#### Scenario: Executor cancel invocation happens after runtime readiness gating
- **WHEN** the runtime invokes executor cancellation for child execution `e1`
- **THEN** the runtime SHALL already have observed `exec/state/e1.json` at `lifecycle = "cancel-ready"`

### Requirement: Cancel-path return values SHALL remain advisory only
Executors SHALL return a success or failure indication from cancel handling, but the runtime SHALL continue to own execution-record lifecycle persistence, including `cancel-ready` and `canceled`.

#### Scenario: Executor return does not own cancel-ready or canceled persistence
- **WHEN** an executor returns from one cancel-path invocation
- **THEN** that return SHALL NOT itself define or persist execution-record lifecycle values such as `cancel-ready` or `canceled`
