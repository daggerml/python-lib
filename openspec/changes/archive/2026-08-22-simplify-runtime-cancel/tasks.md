## 1. Core Cancellation

- [x] 1.1 Replace mode dispatch and sequential advisory cancellation with one concurrent bounded-retry driver, and verify contract tests cover parallel rounds, strict `cancelled` success, selective retries, exhaustion, and resumed `cancel-pending` work.
- [x] 1.2 Persist cancellation `retry_after_ms` as `driver.not_before`, wait for eligibility before each request, hold the execution lock across each adapter invocation and response update, and verify timing, serialization, and guaranteed-release tests.
- [x] 1.3 Remove obsolete cancellation summary buckets and helper branches, and verify the production cancellation implementation has a substantial net line reduction without replacement abstractions.

## 2. Public Surfaces

- [x] 2.1 Change `Dml.runtime.cancel` and `Dag.cancel` to accept validated `max_retries=3`, remove `mode`, and verify Python and generated CLI contract tests cover the new signatures and failure behavior.
- [x] 2.2 Update internal cancellation rendezvous callers to use the unified operation, and verify core mutation-gate cancellation tests pass.

## 3. Executors And Documentation

- [x] 3.1 Make built-in executors report `cancelled` only after successful teardown, preserve idempotency, and verify executor contract tests cover success and failure propagation.
- [x] 3.2 Update runtime, adapter, executor, CLI, and authoring documentation to describe bounded parallel cancellation and verify documentation searches contain no supported `full` or `drive` cancellation modes.

## 4. Verification

- [x] 4.1 Run `openspec validate simplify-runtime-cancel --strict` and resolve all artifact errors.
- [x] 4.2 Run `uv run --dev --all-extras pytest .` and `uv run --dev --all-extras ruff check .`, and verify the full suite and lint pass.
