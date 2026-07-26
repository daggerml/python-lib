## ADDED Requirements

### Requirement: Execution coordination retries SHALL not silently abandon CAS mutations
For child-registration and terminal-child bookkeeping mutations, the execution-state layer SHALL retry compare-and-swap conflicts with bounded exponential backoff. Retry exhaustion SHALL be returned to the calling workflow as an error rather than being logged and treated as success.

#### Scenario: Registration conflict is retried from the latest record
- **WHEN** a child-registration CAS update conflicts
- **THEN** the runtime SHALL reread the caller execution record before retrying
- **AND** it SHALL evaluate the latest lifecycle before attempting the next update

#### Scenario: Exhaustion is observable to the caller
- **WHEN** a bounded execution-record coordination retry budget is exhausted
- **THEN** the execution-state layer SHALL raise an error to its caller
- **AND** it SHALL not return a successful coordination result
