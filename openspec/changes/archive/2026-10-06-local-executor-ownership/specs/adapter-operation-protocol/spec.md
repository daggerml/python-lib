## ADDED Requirements

### Requirement: Ephemeral polling invocation drivers SHALL retain private continuation state

A nested adapter CLI invocation using `--poll` SHALL retain returned adapter state privately through repeated invoke and required nested cleanup calls. A returned state field, including terminal invoke state, SHALL replace the current private state; an omitted field SHALL preserve the prior state and explicit null SHALL clear it. Cleanup retries SHALL use their latest private continuation. After completing this private invocation lifecycle, the CLI SHALL omit `adapter_state` from its terminal output to the wrapper. Ordinary CLI calls without `--poll` SHALL preserve state forwarding, and requests outside this private invocation lifecycle SHALL retain their existing response contract.

#### Scenario: Invoke retries reuse private continuation
- **WHEN** a nested `--poll` invoke returns retry with object state
- **THEN** the next invoke receives that state
- **AND** the driver does not emit an intermediate response to its wrapper

#### Scenario: Terminal invoke state is used for cleanup
- **WHEN** a successful terminal invoke returns updated adapter state
- **THEN** nested cleanup receives the updated state rather than the preceding retry's state

#### Scenario: Omitted terminal state retains the last continuation
- **WHEN** a terminal invoke omits adapter state after a prior retry
- **THEN** the driver retains the preceding private state for cleanup

#### Scenario: Explicit null clears continuation
- **WHEN** a terminal invoke explicitly returns null adapter state
- **THEN** the driver supplies null state to cleanup rather than reusing a preceding retry state

#### Scenario: Cleanup retries remain private
- **WHEN** nested cleanup returns retry with updated state
- **THEN** the next cleanup receives that state
- **AND** the state is not emitted to the wrapper when cleanup finishes

#### Scenario: Terminal private invocation output hides continuation
- **WHEN** the `--poll` invocation lifecycle finishes with a terminal success or failure outcome
- **THEN** its output contains that invocation status and diagnostics without adapter state

#### Scenario: Ordinary CLI response forwards continuation
- **WHEN** the adapter CLI is called without `--poll`
- **THEN** its output retains the operation's returned adapter state unchanged

### Requirement: Docker terminal responses SHALL preserve wrapper continuation

When Docker obtains a valid terminal nested invocation response, it SHALL forward the nested invocation outcome while returning its own saved adapter state to its caller. It SHALL NOT substitute nested adapter state for wrapper owner, container ID, temporary image reference, or other wrapper continuation fields. Error responses produced with valid wrapper launch state SHALL also preserve that state. Nested output validation SHALL retain the existing adapter response contract.

#### Scenario: Nested success retains Docker ownership and resource identifiers
- **WHEN** Docker reads a valid successful nested response that omits adapter state
- **THEN** Docker returns success with its original wrapper state
- **AND** later cleanup receives the wrapper owner and container/image identifiers

#### Scenario: Nested failure retains Docker cleanup state
- **WHEN** Docker reads a valid terminal failure from its nested adapter
- **THEN** it forwards the failure status and error with its own wrapper state

#### Scenario: Child continuation cannot replace Docker state
- **WHEN** a valid terminal nested response includes child adapter state
- **THEN** Docker does not expose that child state as its own continuation
- **AND** it returns its original wrapper state
