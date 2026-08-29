## 1. Split Execution Persistence

- [x] 1.1 Define exact metadata, semantic state, and driver typed schemas and validators with no schema-version or legacy unified-record support.
- [x] 1.2 Replace unified execution keys with `metadata.json`, `state.json`, and `driver.json` storage helpers, including complete reservation-before-cache publication and conditional loser cleanup.
- [x] 1.3 Add bounded deadline-based CAS helpers with exponential jitter for semantic state and owner-checked driver mutations; retain S3-response-time driver lock expiry.
- [x] 1.4 Add fast execution-state contracts covering exact schemas, partial/legacy record rejection, reservation races, stale owner behavior, independent state publication, semantic convergence, and retry exhaustion.

## 2. Runtime State Ownership

- [x] 2.1 Change funk execution commit to upload the DAG and guarded-CAS only `result_ref` plus `result_source = "runtime"`, without acquiring the driver lock or changing lifecycle.
- [x] 2.2 Update caller driving to transition runtime results to `succeeded` and atomically publish adapter-error DAGs with `result_source = "adapter-error"` and lifecycle `failed`.
- [x] 2.3 Move mutation guards, cancelation, invalidation, forward lineage summaries, cache resolution, graph description, and direct execution inspection to the split metadata/state/driver model.
- [x] 2.4 Update cache-hit flow so reusable results remain independent from cleanup status while eligible callers still offer pending cleanup a coordinated drive.
- [x] 2.5 Add core contracts and slow integration coverage for result publication during a held driver lock, result/cancelation races, adapter-error caching, shared executions, graph inspection, invalidation, and cache reuse with pending or failed cleanup.

## 3. Adapter Protocol And Backpressure

- [x] 3.1 Replace invoke response handling with `success`, `retry`, and failure-code validation; require resumable object state for retry and nonempty diagnostics for failures.
- [x] 3.2 Add the exact cleanup request/response types and adapter dispatch, reject `poll`, and replace `ExecutorBase.gc` with idempotent `cleanup` while retaining invoke start/status inference from adapter state.
- [x] 3.3 Persist retry delays as shared absolute `driver.not_before` timestamps, derive the next operation from current state after expiry, clear delays on non-retry responses, and bypass delays for cancelation.
- [x] 3.4 Add protocol contracts for strict cleanup payloads, malformed output, invoke success without a result, hinted and default shared backpressure, operation derivation after result publication, stale-lock responses, and cleanup outcomes that never mutate lifecycle.

## 4. Built-In Leaf Executors

- [x] 4.1 Update the local adapter and script executor to the new statuses and explicit cleanup; move supervisor reaping and work-directory deletion out of invoke status inspection.
- [x] 4.2 Update the Lambda adapter to pass cleanup and classify reliably identifiable provider throttling as retry with provider delay metadata when available.
- [x] 4.3 Add focused contracts for local dispatch, script cleanup retry/success/idempotency, Lambda cleanup forwarding, and transient-versus-terminal Lambda response classification.

## 5. Built-In Wrapper Executors

- [x] 5.1 Update Docker invoke/status handling to retain resources and implement cleanup that safely waits for finalization before idempotently removing the container and temporary image.
- [x] 5.2 Update Batch invoke/status handling to retain resources and implement cleanup that safely handles active jobs, deregisters temporary job definitions, and maps identifiable throttling to shared retry.
- [x] 5.3 Update SSH to use durable nested adapter state across fresh invoke calls and forward explicit cleanup without a separate poll operation or permanently polling SSH connection.
- [x] 5.4 Update the adapter CLI internal polling mode used by ephemeral Docker and Batch environments so nested cleanup completes or terminally records failure before the environment exits.
- [x] 5.5 Add focused wrapper contracts plus slow integration tests proving published results still lead to nested and outer cleanup, retries do not terminate required finalization, repeated cleanup is harmless, and cancelation teardown remains intact.

## 6. Public Contracts And Documentation

- [x] 6.1 Update public execution-record inspection types and serialization to return exact `{metadata, state, driver}` sections while preserving existing execution `Ref` boundaries.
- [x] 6.2 Update runtime/cache, architecture, adapter operation, executor lifecycle, plugin API, and extension concept documentation for split ownership, invoke-only polling, response codes, backpressure, and explicit cleanup.
- [x] 6.3 Update extension status/contract checks so executor plugins require cleanup and built-in adapter/executor diagnostics reflect the new operation surface.

## 7. Verification

- [x] 7.1 Run focused core execution-state, runtime-record, cache, cancelation, invalidation, and call-edge contract suites; require all split-storage and concurrency assertions to pass.
- [x] 7.2 Run focused contrib adapter/executor contracts and slow funk lifecycle integrations; require cleanup, nested wrapper, backpressure, adapter-error, and cancelation scenarios to pass.
- [x] 7.3 Run `uv run --dev --all-extras pyright`, `uv run --dev --all-extras ruff check --fix .`, and `uv run --dev --all-extras pytest -m "not slow" .`; inspect formatter changes and resolve every failure attributable to the change.
- [x] 7.4 Run `uv run --dev --all-extras pytest .` and `openspec validate split-execution-state-cleanup --strict`; require the complete suite and change validation to pass.
