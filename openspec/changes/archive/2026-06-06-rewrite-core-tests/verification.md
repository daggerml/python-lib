## Verification

- Targeted new `_core` rewrite tests: `uv run --dev --all-extras pytest tests/contracts/test_core_head_refs.py tests/contracts/test_core_revision_selectors.py tests/contracts/test_core_serde_values.py tests/contracts/test_core_types_contracts.py tests/contracts/test_core_config_resolution.py tests/contracts/test_core_execution_coordination.py tests/integration/test_core_parallel_init_integration.py tests/integration/test_core_parallel_runtime_integration.py tests/integration/test_core_parallel_branch_commits_integration.py` -> 72 passed.
- Fast-path selection: `time uv run --dev --all-extras pytest -m "not slow" tests/contracts/test_core_head_refs.py tests/contracts/test_core_revision_selectors.py tests/contracts/test_core_serde_values.py tests/contracts/test_core_types_contracts.py tests/contracts/test_core_config_resolution.py tests/contracts/test_core_execution_coordination.py tests/integration/test_core_parallel_init_integration.py tests/integration/test_core_parallel_runtime_integration.py tests/integration/test_core_parallel_branch_commits_integration.py` -> 72 passed in 0.92s wall time.
- Contributor repository command: `uv run --dev --all-extras pytest .` -> 73 passed.

## Production Defects Exposed

- Empty index namespace reads caused `Dml.status()` and `runtime.list()` to raise `DmlDbKeyNotFoundError`; fixed by treating a missing `index` namespace as empty and hydrating iterated index refs before returning runtime list payloads.
- Concurrent `Dml.init(...)` could read a partially written `.dml/config.json`; fixed config writes to use a temp file plus atomic `os.replace()`.
