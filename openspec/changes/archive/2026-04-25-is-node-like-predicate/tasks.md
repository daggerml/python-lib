## 1. Add is_node_like predicate

- [x] 1.1 In `src/daggerml/contrib/api.py`, add `is_node_like(x: object) -> bool` after the `Delayed*` class definitions, returning `isinstance(x, (Node, DelayedRef, DelayedLoad, DelayedRunnable))`
- [x] 1.2 Export `is_node_like` in any relevant `__all__` or public re-export in `contrib/api.py`

## 2. Update SshExecutor validation

- [x] 2.1 In `src/daggerml/contrib/executors/ssh.py`, add `is_node_like` to the import from `daggerml.contrib.api`
- [x] 2.2 In `SshExecutor._validate_kw`, replace `isinstance(host, DelayedActionCodec)` with `is_node_like(host)`
- [x] 2.3 In `SshExecutor._validate_kw`, replace `isinstance(flags, DelayedActionCodec)` with `is_node_like(flags)`

## 3. Verify

- [x] 3.1 Run existing tests to confirm no regressions (`pytest` or equivalent)
- [x] 3.2 Confirm `is_node_like` returns `False` for `DelayedActionCodec` instances (manual check or test)
