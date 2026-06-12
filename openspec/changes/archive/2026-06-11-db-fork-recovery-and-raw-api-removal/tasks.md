## 1. Remove dead raw DB API surface

- [x] 1.1 Remove `raw=True` support from `src/daggerml/_core/db.pyx` transaction `get()` and `put()` paths.
- [x] 1.2 Remove typed `get_raw()` and `put_raw()` helpers from `src/daggerml/_core/types.py` and update any affected type annotations or callers.

## 2. Add seamless handle-level fork recovery

- [x] 2.1 Add a private `DmlDb` reopen helper that replaces `self._handle` using the stored DB configuration.
- [x] 2.2 Route handle-level DB operations through retry-on-fork logic that replaces the handle and retries once on fork-invalidated C return codes.
- [x] 2.3 Keep inherited transaction objects fail-fast by preserving transaction-level fork errors rather than auto-recreating transactions.

## 3. Update tests and validate behavior

- [x] 3.1 Update DB concurrency/fork integration tests to assert child processes can reuse the same logical DB facade without manual reopen.
- [x] 3.2 Add or update coverage for inherited transaction objects remaining invalid after fork.
- [x] 3.3 Run the relevant `_core` DB test suite and confirm the new fork-recovery behavior passes.
