## Context

`daggerml.contrib.supervisor` currently launches a worker subprocess, redirects worker `stdout` and `stderr` to local files, waits for exit, and returns a terminal result based on `result.json` or the worker exit status. That shape is useful and should remain intact, but it provides no durable live observability for long-running or remote executions because the logs exist only in a temporary workdir and are removed during script-executor cleanup.

The repository already depends on `boto3`, so CloudWatch Logs integration can be added without introducing a new package or changing the runtime ownership model. The user requirement also calls for safe fallback behavior: log shipping must never become part of execution correctness.

## Goals / Non-Goals

**Goals:**
- Preserve the current supervisor/executor control flow and terminal result contract.
- Stream worker `stdout` and `stderr` to CloudWatch Logs while the worker is still running.
- Keep writing local `stdout.log` and `stderr.log` files for fallback and local debugging.
- Emit start and end lifecycle messages that include `execution_id`, `cache_key`, stream kind, and terminal status where applicable.
- Make CloudWatch failures best-effort so worker execution still completes normally when log shipping fails.

**Non-Goals:**
- Changing adapter output, result publication, or polling semantics.
- Introducing a new dependency such as `watchtower` or an external `aws` CLI requirement.
- Generalizing this change to every executor in the same change.
- Making CloudWatch configuration dynamically user-selectable beyond the fixed group and stream naming required by this change.
- Adding a backward-compatibility code path, legacy supervisor logging mode, or stream-name shim layer.

## Decisions

### Use the supervisor as the CloudWatch log owner
The supervisor already owns worker process launch, local log capture, and terminal result interpretation. Extending it to own CloudWatch streaming keeps observability close to the existing process boundary and avoids spreading log-shipping concerns into polling or higher runtime layers.

Alternative considered: ship logs from the script executor during `poll()`. Rejected because polling is intermittent rather than continuous, complicates state ownership, and cannot provide true live streaming.

### Read worker `stdout` and `stderr` through pipes and tee them to local files plus CloudWatch
To stream logs while the worker runs, the supervisor should launch the worker with `stdout=PIPE` and `stderr=PIPE`, consume each pipe concurrently, append the bytes to the existing local log files, and batch line-oriented CloudWatch events for the corresponding stream.

This replaces the existing direct worker-to-file redirection path in the supervisor rather than preserving a separate legacy implementation branch. Local file capture remains part of the single active path, not a compatibility mode.

Alternative considered: keep file redirection and tail the files from background threads. Rejected because it adds file-offset bookkeeping and weaker real-time behavior without simplifying the code meaningfully.

### Use one CloudWatch log stream per output channel
The log group is fixed to `dml`, and the stream names are fixed to `/run/{cache_key}/stdout` and `/run/{cache_key}/stderr`. Separate streams preserve channel identity without interleaving rules or merged timestamps.

The implementation should use those exact stream names and should not introduce a sanitization or aliasing shim. If a computed stream name cannot be used with CloudWatch as-is, CloudWatch delivery for that channel should fail safely and local log capture should continue.

Alternative considered: a single merged log stream. Rejected because preserving stdout/stderr separation would require extra envelope data for every message and would make direct CloudWatch inspection harder.

### Emit explicit lifecycle events at the beginning and end of each stream
Each stream should begin with a metadata event describing the execution and stream kind, and end with a metadata event describing the same execution plus terminal status. These events provide stable anchors even when the worker itself emits no output.

Alternative considered: rely only on raw worker output. Rejected because silent workers would produce no CloudWatch evidence that streaming was configured or completed.

### Make CloudWatch delivery best-effort and self-disabling on failure
If CloudWatch client creation, log group/stream setup, or `put_log_events` fails, the supervisor should record the problem locally and continue writing worker output to local files. Repeated CloudWatch failures for a stream should disable further CloudWatch writes for that stream instead of repeatedly failing in the hot path.

Alternative considered: fail the supervisor when CloudWatch initialization fails. Rejected because it would turn observability into a correctness dependency and violate the fallback requirement.

## Risks / Trade-offs

- CloudWatch stream writes add thread and batching complexity to the supervisor -> Keep the implementation narrow: one reader thread per pipe, one CloudWatch sink per stream, and no changes to executor polling semantics.
- `cache_key` may contain characters or length patterns that CloudWatch stream names may reject -> Do not add a name-rewriting shim; let CloudWatch delivery disable itself for that channel and preserve local log capture.
- Buffered or partial worker output may not align perfectly with line boundaries -> Buffer partial lines in the reader thread and flush any remainder on EOF.
- CloudWatch API throttling or transient failures can drop streamed logs -> Keep logging best-effort, flush final buffered events on shutdown, and preserve the local files as the fallback record.
- Start and end lifecycle messages may be duplicated if retry logic is too broad -> Make lifecycle emission stream-local and idempotent within a single supervisor run.

## Migration Plan

No persisted data migration is required.

Implementation rollout is additive:
1. Add supervisor-side CloudWatch streaming in the single supervisor launch path defined by this change.
2. Extend integration and unit coverage for local fallback, lifecycle events, and non-fatal CloudWatch failures.
3. Update contrib runtime docs to describe the new observability behavior.

Rollback is straightforward: remove or disable the supervisor CloudWatch streaming path and retain the existing local log-file capture behavior.

## Open Questions

- What exact event message shape should be used for lifecycle messages: plain text with embedded metadata or compact JSON payloads.
- Whether the final terminal metadata event should include the supervisor return classification only, or also worker exit-code/signal details when available.
