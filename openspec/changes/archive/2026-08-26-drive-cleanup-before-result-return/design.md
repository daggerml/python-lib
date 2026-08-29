## Context

`ExecutionState.get_or_start_fn()` owns the driver lock while handling invoke responses. Cached terminal results use `_drive_cleanup()`, which acquires its own lock, but fresh terminal paths currently return while still holding their existing lock and bypass cleanup. Cleanup state is already represented by nullable `driver.cleanup`; retry continuation uses `driver.adapter_state` and `driver.not_before`.

## Goals / Non-Goals

**Goals:**

- Use one cleanup decision for cached and freshly established terminal results.
- Attempt required, eligible cleanup once before returning a result.
- Preserve current cleanup response and backpressure semantics.

**Non-Goals:**

- Waiting synchronously for cleanup retries to become terminal.
- Changing adapter payloads, responses, executor signatures, or execution record schemas.
- Adding a background cleanup reconciler.

## Decisions

### Separate lock ownership from cleanup response handling

Factor the cleanup drive into an operation that accepts an existing driver owner. Keep a wrapper that acquires and releases the lock for cache lookups; invoke handling calls the owned operation directly. This avoids trying to reacquire the non-reentrant distributed lock already held by `get_or_start_fn()`.

Alternative: unlock and call the existing cleanup driver. Rejected because another caller could take ownership between terminal result establishment and the required cleanup attempt.

### Derive cleanup requirement from durable state

Cleanup is required exactly when `result_ref` is populated and `driver.cleanup` is null. A `complete` or `failed` record is terminal and suppresses repeated cleanup. `driver.not_before` controls eligibility without changing whether cleanup remains required.

Alternative: add a separate required flag. Rejected because the existing fields fully determine the operation and a new field would create redundant state.

### Drive cleanup once per result-returning call

Both cached and fresh terminal paths perform at most one eligible cleanup adapter call before returning. Success records complete, provider failure records failed diagnostics, and retry persists adapter state and `not_before` while leaving cleanup null. The result remains returnable for every cleanup outcome.

Alternative: loop until cleanup becomes terminal. Rejected because cleanup backpressure could block result delivery indefinitely. Ephemeral nested adapter polling retains its separate requirement to finish nested cleanup before its environment exits.

### Establish the result before cleanup

A successful invoke first finalizes the runtime-published result. A provider failure first creates and stores its adapter-error DAG. Cleanup then receives the resulting non-null DAG ref. Caller-child terminal bookkeeping remains semantic completion and does not depend on cleanup success.

## Risks / Trade-offs

- [Cleanup adds latency to the first result return] → Limit the path to one eligible adapter call and preserve retry deferral.
- [Refactoring lock handling could duplicate unlocks or adapter calls] → Cover owned and acquired paths with exact operation-order tests.
- [Cleanup retry shares adapter continuation state with terminal invoke] → Persist the cleanup response only after invoke state has been stored, matching the existing single continuation field contract.

## Migration Plan

No data migration is required. Existing records with null cleanup become eligible on their next result-returning call; records with complete or failed cleanup remain unchanged. Rollback restores demand-driven cache-hit cleanup without changing persisted schema.
