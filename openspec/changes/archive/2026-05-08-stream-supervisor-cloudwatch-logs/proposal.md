## Why

Supervisor-backed script executions currently capture worker `stdout` and `stderr` only in local temporary files. That makes live debugging difficult for remote or long-running executions, and the logs disappear when the executor cleans up its workdir.

## What Changes

- Add best-effort CloudWatch Logs streaming for supervisor-managed worker `stdout` and `stderr`.
- Keep the current supervisor/executor structure: the supervisor still launches the worker, captures local log files, waits for terminal completion, and returns the same terminal result contract.
- Stream logs concurrently while the worker runs so polling and execution progress can be observed in near real time.
- Use log group `dml` and per-run log streams `/run/{cache_key}/stdout` and `/run/{cache_key}/stderr`.
- Emit structured lifecycle messages at stream start and stream end containing execution metadata such as `execution_id`, `cache_key`, and terminal status.
- Make CloudWatch failures non-fatal and fall back safely to local `stdout`/`stderr` capture.
- Implement the change as a single supervisor path with no backward-compatibility branch, no legacy logging mode, and no name-rewriting shim for CloudWatch stream names.

## Capabilities

### New Capabilities
- `supervisor-cloudwatch-streaming`: Best-effort streaming of supervisor-managed worker `stdout` and `stderr` to CloudWatch Logs with start/end lifecycle messages and safe fallback behavior.

### Modified Capabilities

## Impact

- Affected code: `src/daggerml/contrib/supervisor.py`, `src/daggerml/contrib/executors/script.py`, and related contrib integration tests.
- Affected docs/specs: contrib runtime and executor behavior docs, plus a new OpenSpec capability for supervisor log streaming.
- Dependencies: no new package dependencies; implementation uses the existing `boto3` runtime dependency.
- Systems: AWS CloudWatch Logs for observability, with unchanged execution correctness when CloudWatch is unavailable or misconfigured.
