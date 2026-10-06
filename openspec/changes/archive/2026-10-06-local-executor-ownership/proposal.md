## Why

When multiple machines share an execution cache, a caller can receive another machine's script PID or Docker container ID and incorrectly inspect or tear down its own local resources. Detached wrapper execution also leaks nested continuation state and can replace a successful invocation outcome with a cleanup failure.

## What Changes

- Add a flat execution-environment `owner` identifier to script and Docker launch state, persisted through the existing adapter-state protocol.
- Gate script and Docker poll, cleanup, and cancellation on that owner; a different environment returns retry with unchanged state without touching local resources.
- Keep script execution identical inside and outside containers. The nested adapter's `--poll` driver privately retains continuation state through invoke and cleanup, then omits it from its terminal output.
- Preserve Docker's own state when returning a nested invocation outcome.
- Preserve the invocation outcome independently of nested cleanup; report cleanup failure on stderr without converting published success into invocation failure.
- Leave normal, non-`--poll` CLI state forwarding and SSH transport unchanged. Do not add ownership restrictions to Batch.

## Capabilities

### New Capabilities

- `local-executor-ownership`: execution-environment ownership for script and Docker state and safe handling of requests from another environment.

### Modified Capabilities

- `adapter-operation-protocol`: private continuation handling and terminal state omission for the nested `--poll` invocation driver, plus preservation of Docker wrapper state.
- `executor-cancellation`: nested cleanup completion and diagnostics remain independent of the terminal invocation outcome.

## Impact

- Implementation: `src/daggerml/contrib/executors/script.py`, `src/daggerml/contrib/executors/docker.py`, `src/daggerml/contrib/adapters.py`, and a small private ownership helper in contrib.
- Coverage: script, nested-executor, adapter CLI, SSH-forwarding, and Batch compatibility contract tests; focused local-process/Docker integration coverage where available.
- Documentation: `docs/extend/adapters.qmd`, `docs/extend/executors.qmd`, and the relevant execution/runtime guidance.
- No new S3 records, core lifecycle changes, wire fields, runnable/cache identity changes, dependencies, or nested adapter-state schema.
- **BREAKING**: script and Docker continuation state requires `owner` formatted as `effective-UID@hostname`; ownerless or hostname-only state is unsupported, with no backward-compatibility path. Distinct hosts/containers require distinct hostnames.
