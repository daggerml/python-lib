## 1. Supervisor CloudWatch streaming

- [x] 1.1 Add supervisor-side CloudWatch logging helpers that target log group `dml` and streams `/run/{cache_key}/stdout` and `/run/{cache_key}/stderr` using the existing `boto3` dependency.
- [x] 1.2 Change the supervisor worker launch path to read `stdout` and `stderr` from pipes, tee each channel into the existing local log files, and stream each channel concurrently while the worker runs, without keeping a legacy alternate path.
- [x] 1.3 Emit per-stream lifecycle messages at startup and shutdown that include `execution_id`, `cache_key`, stream kind, and terminal status, and make CloudWatch failures self-disabling and non-fatal without adding compatibility aliases or stream-name shims.

## 2. Runtime behavior and tests

- [x] 2.1 Preserve the existing supervisor terminal result behavior while ensuring CloudWatch streaming flushes and shuts down cleanly after worker exit.
- [x] 2.2 Add or update supervisor and script-executor tests for combined live stdout/stderr capture and local-file preservation.
- [x] 2.3 Add failure-path tests covering CloudWatch initialization or delivery errors to verify execution still succeeds or fails based only on the worker terminal result.

## 3. Documentation and verification

- [x] 3.1 Update the contrib runtime and executor docs to describe supervisor-managed CloudWatch log streaming and its best-effort fallback behavior.
- [x] 3.2 Run the targeted test coverage for supervisor and contrib script execution paths, and fix any regressions introduced by the streaming change.
