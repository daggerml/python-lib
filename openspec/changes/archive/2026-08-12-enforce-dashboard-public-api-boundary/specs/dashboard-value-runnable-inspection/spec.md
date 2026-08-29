## ADDED Requirements

### Requirement: Runnable evidence excludes executor-local resource inspection
The shared runnable presentation SHALL retain the persisted runnable stack and
safe executor configuration. Where persisted execution launch state is available,
it SHALL expose that state only as bounded, redacted, non-authoritative JSON.
The presentation SHALL NOT inspect or display local executor logs, process
status, Docker container state, Batch job state, CloudFormation state, or other
executor-specific live resource probes.

#### Scenario: Runnable has persisted launch state
- **WHEN** a user inspects a Runnable whose execution has persisted launch state
- **THEN** the dashboard presents bounded redacted launch-state JSON with the
  runnable evidence
- **AND** it does not infer executor lifecycle or resource health from that JSON

#### Scenario: Runnable has no launch state
- **WHEN** a user inspects a Runnable without available persisted launch state
- **THEN** the dashboard presents bounded unavailable evidence
- **AND** it does not probe an executor or host for substitute status
