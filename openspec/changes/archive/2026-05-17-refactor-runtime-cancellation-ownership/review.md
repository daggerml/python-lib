## Review Findings

### 1. Cache-key lock can leak on cancellation races

`IndexOps.start_fn()` only unlocks around `_call_adapter()` exceptions, but both `_record_call_edges()` and the subsequent `es.update_execution_record(...)` can raise `CancelledExecutionError` via `ExecutionState.update_execution_record()` when the record has already flipped to a `cancel-*` lifecycle. In that path the function exits without `es.unlock()`, leaving the lock held until TTL expiry and stalling future launches for that cache key.

- Files:
  - `src/daggerml/_internal/ops/index.py:181-199`
  - `src/daggerml/_internal/exec_state.py:380-394`

### 2. `dml.runtime.cancel()` now fails healthy long-running cancellations after three retries

The new retry cap applies not just to transport errors, but also to normal `outcome is None` cases and lock contention. A long-running adapter-side cancel or a briefly busy lock now raises `DmlRepoError` even though cancellation is progressing correctly out of band. The agreed deviation was adapter-call retries; this loop-level retry budget changes cancellation semantics more broadly.

- File:
  - `src/daggerml/_internal/dml.py:569-635`

### 3. Cancellation still invokes adapters synchronously while holding the callee lock

`_cancel_execution_candidate()` deletes the active pointer, updates the record, drops child edges, and calls `_invoke_cancel_update()` before releasing the callee cache-key lock. That reintroduces the original ownership and permission problem the refactor was meant to remove, and blocks new callers on the same cache key for the full adapter-call duration.

- Files:
  - `src/daggerml/_internal/ops/index.py:919-995`
  - `src/daggerml/_internal/ops/index.py:1032-1053`

### 4. `CancelledExecutionError` still inherits from `DmlRepoError`

The design called for a cancellation interruption that is not a `daggerml.Error`. Keeping `CancelledExecutionError` as a `DmlRepoError` means it still sits inside the normal error hierarchy and risks being treated like a domain or repository failure instead of a distinct control-plane interruption.

- Files:
  - `src/daggerml/_internal/exec_state.py:35`
  - `src/daggerml/_internal/types.py:455`

### 5. Stale active-pointer recovery now crashes instead of relaunching

In `start_fn`, the runtime clears `execution_id` after detecting that the active execution is stale because `launch_state` is missing or the lifecycle is terminal, but then immediately asserts `execution_record is None`. That assertion is false in the stale-pointer cases where the `execution_record` still exists, so a recoverable relaunch path turns into an `AssertionError`.

- File:
  - `src/daggerml/_internal/ops/index.py:144-156`

### 6. `dml.runtime.cancel()` no longer paces normal retry loops

The current implementation only applies backoff when `_cancel_execution_candidate()` raises an exception. Normal cancellation-progress cases such as `lock_retry=True` or `outcome is None` immediately spin the outer loop without the planned sleep interval, which can create a hot loop against shared state while waiting for out-of-band cancellation work to progress.

- File:
  - `src/daggerml/_internal/dml.py:581-629`
