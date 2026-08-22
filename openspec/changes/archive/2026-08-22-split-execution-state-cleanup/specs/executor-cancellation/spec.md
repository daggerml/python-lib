## ADDED Requirements

### Requirement: Executors SHALL prune normally completed resources through cleanup
Every executor SHALL expose idempotent cleanup distinct from invoke and cancel. Cleanup SHALL prune external resources after result publication and SHALL return success, retry with resumable state and optional delay, or a failure code with diagnostics. Cleanup SHALL NOT publish results or determine execution lifecycle.

#### Scenario: Active finalization requests retry
- **WHEN** result is published but an executor-owned process or remote job is still finalizing
- **THEN** cleanup returns retry without terminating required finalization work

#### Scenario: Completed resource is pruned
- **WHEN** executor-owned resources are safe to remove
- **THEN** cleanup removes them and returns success

#### Scenario: Repeated successful cleanup is harmless
- **WHEN** cleanup is repeated after resources were removed
- **THEN** it returns success without recreating or corrupting resources

### Requirement: Built-in executors SHALL move normal teardown out of invoke
Normal terminal invoke handling SHALL not be the sole teardown path. Script cleanup SHALL reap completed supervisor work and remove its work directory. Docker cleanup SHALL remove the terminal container and temporary image. Batch cleanup SHALL prune the job's temporary definition and any execution-owned resources after safe terminal observation. SSH cleanup SHALL preserve nested cleanup semantics across fresh remote calls. Cancellation MAY continue to tear down the same resources for canceled work.

#### Scenario: Published result bypasses further invoke
- **WHEN** a result is published before another invoke observes terminal work
- **THEN** explicit cleanup still prunes the built-in executor's resources

#### Scenario: Cancellation teardown remains available
- **WHEN** an execution is selected for cancellation before result publication
- **THEN** executor cancellation tears down its resources without requiring cleanup first

### Requirement: Ephemeral nested adapter drivers SHALL finish nested cleanup
When Docker or Batch runs a nested adapter in an ephemeral environment using its internal polling loop, that nested driver SHALL complete or terminally record nested cleanup before the environment exits. Outer cleanup SHALL independently prune the wrapper container, job, image, or job definition.

#### Scenario: Containerized nested execution completes
- **WHEN** a nested adapter publishes its result inside Docker execution
- **THEN** the nested driver performs nested cleanup before exiting
- **AND** outer Docker cleanup later removes wrapper resources
