### Requirement: Supervisor streams worker stdout and stderr to CloudWatch Logs
The supervisor SHALL stream worker `stdout` and `stderr` to AWS CloudWatch Logs while the worker process is still running, in addition to preserving local `stdout.log` and `stderr.log` files.

#### Scenario: Stdout is streamed while the worker runs
- **WHEN** the supervisor starts a worker that writes to `stdout`
- **THEN** the supervisor writes the output to the local `stdout.log` file and publishes the same output to CloudWatch Logs before the worker exits

#### Scenario: Stderr is streamed while the worker runs
- **WHEN** the supervisor starts a worker that writes to `stderr`
- **THEN** the supervisor writes the output to the local `stderr.log` file and publishes the same output to CloudWatch Logs before the worker exits

### Requirement: Supervisor uses fixed CloudWatch log destinations per run
The supervisor SHALL publish worker logs to log group `dml` and SHALL use exactly two log streams named `/run/{cache_key}/stdout` and `/run/{cache_key}/stderr` for the corresponding worker output channels.

#### Scenario: Stdout stream name is derived from cache key
- **WHEN** the supervisor launches a worker for a given `cache_key`
- **THEN** worker `stdout` events are published to CloudWatch log stream `/run/{cache_key}/stdout` in log group `dml`

#### Scenario: Stderr stream name is derived from cache key
- **WHEN** the supervisor launches a worker for a given `cache_key`
- **THEN** worker `stderr` events are published to CloudWatch log stream `/run/{cache_key}/stderr` in log group `dml`

#### Scenario: Supervisor does not rewrite stream names
- **WHEN** the supervisor computes CloudWatch stream names from `cache_key`
- **THEN** it uses the exact names `/run/{cache_key}/stdout` and `/run/{cache_key}/stderr` without a compatibility alias or sanitization shim

### Requirement: Supervisor emits lifecycle metadata at stream start and end
The supervisor SHALL emit a lifecycle event to each CloudWatch log stream when streaming begins and another lifecycle event when streaming ends. Lifecycle events SHALL include `execution_id`, `cache_key`, the stream kind (`stdout` or `stderr`), and the terminal status when streaming ends.

#### Scenario: Start lifecycle event is emitted before worker output
- **WHEN** the supervisor initializes CloudWatch streaming for a worker output channel
- **THEN** it first publishes a lifecycle event containing the execution metadata for that channel before publishing worker output events

#### Scenario: End lifecycle event is emitted after worker exit
- **WHEN** the worker process has exited and the supervisor has determined the terminal result
- **THEN** it publishes a lifecycle event containing the execution metadata and terminal status for each channel before closing CloudWatch streaming

### Requirement: CloudWatch failures do not fail worker execution
CloudWatch client, log-stream, or event-delivery failures SHALL be non-fatal to execution. When CloudWatch streaming fails, the supervisor SHALL continue capturing worker output locally and SHALL continue evaluating the worker terminal result using the existing supervisor result contract.

#### Scenario: CloudWatch initialization fails
- **WHEN** the supervisor cannot initialize CloudWatch logging for a worker output channel
- **THEN** the supervisor continues the worker run, preserves local log-file capture, and still returns the worker terminal result normally

#### Scenario: CloudWatch delivery fails after streaming has started
- **WHEN** CloudWatch event delivery fails during an active worker run
- **THEN** the supervisor continues capturing output locally for the rest of the run and still returns the worker terminal result normally
