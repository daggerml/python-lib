## 1. C Registry Core

- [x] 1.1 Add a fixed-size process-local registry in `c/src/dml_db.c` keyed by canonical DB path and guarded by a mutex.
- [x] 1.2 Change `dml_db_open`/`dml_db_close` semantics so handles become lightweight registry tokens instead of persistent env owners.
- [x] 1.3 Add slot lookup, slot allocation, slot clearing, and PID-mismatch registry reset behavior.

## 2. Env Lease Lifecycle

- [x] 2.1 Update transaction-open paths to acquire or create a slot env, increment the slot refcount, and begin the LMDB transaction.
- [x] 2.2 Update transaction-close paths to decrement the slot refcount and close the env when the count reaches zero.
- [x] 2.3 Remove live resize behavior and replace map-full recovery with reopen-at-larger-map-size retry logic.

## 3. Python Binding Updates

- [x] 3.1 Update `src/daggerml/_core/db.pyx` to match the new C handle and transaction lifecycle semantics.
- [x] 3.2 Update `src/daggerml/_core/types.py` and related callers so higher-level DB wrappers continue to work without owning persistent envs.

## 4. Verification

- [x] 4.1 Add tests for same-path handle deduplication, concurrent init stability, and final-lease env close behavior.
- [x] 4.2 Add tests for PID reset after fork, map-full reopen-and-retry behavior, and registry-capacity failures.
- [x] 4.3 Run the relevant core contract and integration tests for DB lifecycle and parallel init flows.
