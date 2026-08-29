## 1. Contract Coverage

- [x] 1.1 Replace the fresh-success cleanup xfail with passing coverage that asserts `invoke`, then `cleanup`, then result return in one `get_or_start_fn()` call.
- [x] 1.2 Add execution-coordination cases for cached and fresh failed results, cleanup retry persistence, retry-delay deferral, and skipping cleanup already recorded as complete or failed.

## 2. Runtime Coordination

- [x] 2.1 Refactor cleanup driving into an owner-held operation plus an acquire/release wrapper, preserving current cleanup success, retry, failure, and backpressure updates.
- [x] 2.2 Route cached results and every fresh terminal invoke result through required, eligible cleanup before returning, without waiting for cleanup retries.

## 3. Documentation And Verification

- [x] 3.1 Update runtime and adapter lifecycle documentation to state the shared cached/fresh cleanup-before-return contract.
- [x] 3.2 Run formatting, lint, type checks, and the non-slow test suite; confirm the OpenSpec change validates strictly.
