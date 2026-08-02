## 1. Native Resize Coordination

- [x] 1.1 Add per-registry-slot resize state and condition-variable coordination in the native DB registry.
- [x] 1.2 Add an explicit native resize API that blocks new acquisitions, waits for active leases to drain, and reopens the environment at a requested larger map size.
- [x] 1.3 Preserve ordinary transaction-open semantics: an already-open environment ignores `map_size` and does not enter the resize gate.
- [x] 1.4 Detect LMDB map-resized acquisition failures, coordinate a local reopen at the backing map size, and retry acquisition once.
- [x] 1.5 Add C/Cython declarations and Python bindings for explicit resize and any required map-size inspection or terminal-capacity context.

## 2. Growth-Aware Write API

- [x] 2.1 Replace the one-shot `call_with_resize` behavior with internal `write_with_growth(fn)` behavior that aborts and retries on map-full from operations or commit.
- [x] 2.2 Implement native map-size probing and fixed-headroom growth capped by the configured maximum, including overflow-safe and no-progress handling.
- [x] 2.3 Raise a contextual terminal capacity error when no larger permitted map size remains; preserve other native resize errors.
- [x] 2.4 Expose the typed `DmlDB` wrapper for `write_with_growth(fn)` and update type stubs.

## 3. Migrate Replayable Local Writes

- [x] 3.1 Audit core write transactions and move pure graph, commit, GC, and local repository mutations to `write_with_growth(fn)`.
- [x] 3.2 Refactor adapter-backed `IndexOps.start_fn` into replayable local preparation, external coordination, and replayable local attachment phases.
- [x] 3.3 Move remote object materialization and adapter-error DAG persistence into replayable local write functions without replaying network or adapter effects.
- [x] 3.4 Verify filesystem pointer updates, remote publication, execution-state updates, timestamps, and generated IDs remain outside retryable functions unless their semantics are explicitly idempotent.

## 4. Test And Document Behavior

- [x] 4.1 Add native/core tests proving an explicit resize waits for active leases and unblocks queued transaction requests after successful reopen.
- [x] 4.2 Add tests proving ordinary transaction opens with `map_size` reuse an open environment without serializing or resizing it.
- [x] 4.3 Add growth-aware write tests for map-full during operation and commit, multiple growth attempts, and terminal configured-capacity failure diagnostics.
- [x] 4.4 Add process-level coverage for adopting a map resized by another process.
- [x] 4.5 Add integration coverage for large DAG mutations and remote materialization under intentionally small initial maps.
- [x] 4.6 Update configuration and error documentation for automatic growth limits and terminal capacity failures, then run required lint, type checks, and non-slow tests.
