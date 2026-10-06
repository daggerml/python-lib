## MODIFIED Requirements

### Requirement: Ephemeral nested adapter drivers SHALL finish nested cleanup

When Docker or Batch runs a nested adapter in an ephemeral environment using its internal polling loop, that nested driver SHALL complete or terminally report nested cleanup before the environment exits. The driver SHALL preserve its terminal invocation outcome independently of cleanup responses. Terminal cleanup failure or a cleanup exception SHALL emit stderr diagnostics identifying the execution and failure and SHALL NOT replace successful invocation status or diagnostics. Outer cleanup SHALL independently prune the wrapper container, job, image, or job definition. Missing result publication after a successful invoke SHALL remain an invocation protocol error, not a cleanup warning.

#### Scenario: Containerized nested execution completes
- **WHEN** a nested adapter publishes its result inside Docker execution
- **THEN** the nested driver performs nested cleanup before exiting
- **AND** outer Docker cleanup later removes wrapper resources

#### Scenario: Nested cleanup retries do not become invocation outcomes
- **WHEN** a successful nested invoke is followed by cleanup retries and eventual cleanup success
- **THEN** the driver retains the successful invocation outcome while finishing cleanup

#### Scenario: Terminal cleanup failure preserves published invocation success
- **WHEN** a successful published nested invoke is followed by terminal cleanup failure
- **THEN** the driver emits stderr diagnostics identifying the execution and cleanup failure
- **AND** its terminal invocation output remains successful

#### Scenario: Cleanup exception preserves published invocation success
- **WHEN** cleanup raises after a successful published nested invoke
- **THEN** the driver reports the cleanup exception on stderr and retains the successful invocation output

#### Scenario: Missing publication is not hidden as cleanup failure
- **WHEN** a nested invoke reports success without a published result
- **THEN** the driver treats the missing publication as an invocation protocol error
- **AND** it does not downgrade that error to a cleanup warning
