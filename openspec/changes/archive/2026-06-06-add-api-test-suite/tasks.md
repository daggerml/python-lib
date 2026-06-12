## 1. Test Scaffolding

- [x] 1.1 Add shared test helpers or fixtures for constructing fake `Dml` objects with `runtime`, `dag`, and `show` behavior suitable for `daggerml.api` contract tests.
- [x] 1.2 Add fixtures that reset `daggerml.api` default-DML globals and codec registry state so tests do not leak process or plugin state.
- [x] 1.3 Define helper refs and realistic namespace payloads for API wrapper tests, using `Ref` objects that match `_core.Dml` namespace boundaries.

## 2. Default Runtime Contract Tests

- [x] 2.1 Add `tests/contracts/test_api_defaults.py` covering implicit, process, scoped, nested scoped, and cleared default `Dml` resolution.
- [x] 2.2 Test top-level `status()` metadata and delegation to the active `Dml.status()` result.
- [x] 2.3 Test `new()` delegation to `runtime.create(...)` and returned `Dag` state.
- [x] 2.4 Test `load()` successful DAG lookup and missing-DAG `DmlRepoError` behavior.
- [x] 2.5 Test `temporary()` construction by mocking `Dml.init` and `Dml` instantiation.

## 3. Dag Contract Tests

- [x] 3.1 Add `tests/contracts/test_api_dag_contracts.py` covering `_require_index_ref`, `_make_node` classification, `put`, and `_put_literal` delegation.
- [x] 3.2 Test named-node access and assignment for uncommitted and committed DAGs, including missing-node and committed-assignment errors.
- [x] 3.3 Test `keys()`, `values()`, `argv`, and `result` behavior for committed and uncommitted DAGs.
- [x] 3.4 Test `require()` for committed DAG result imports, named-node imports, missing DAGs, and missing nodes.
- [x] 3.5 Test `_call_builtin()` and `call()` delegation, retry success, timeout behavior, and function-execution failure handling.
- [x] 3.6 Test `commit()` for raw values, `Node` values, and `Error` values, including final `self.ref` resolution.
- [x] 3.7 Test context-manager exception capture for normal exceptions and existing `Error` values.

## 4. Node Contract Tests

- [x] 4.1 Add `tests/contracts/test_api_node_contracts.py` covering `Node.value`, `Node.load`, `Node.type`, equality, hashing, repr, and non-callable node errors.
- [x] 4.2 Test `RunnableNode.__call__` delegation to `Dag.call`.
- [x] 4.3 Test `CollectionNode.contains`, `__contains__`, and collection length behavior.
- [x] 4.4 Test `ListNode` indexing, slicing, slice-step rejection, iteration, `conj`, and `append` behavior.
- [x] 4.5 Test `DictNode` indexing, copied keys, iteration, `get`, `items`, `values`, `assoc`, and chained `update` behavior.

## 5. Codec Contract Tests

- [x] 5.1 Add `tests/contracts/test_api_codecs.py` covering `codecs()`, entry-point loading, plugin load-once behavior, priority ordering, and plugin failure wrapping.
- [x] 5.2 Test `apply_codec()` first-match behavior, unchanged pass-through, `DmlRepoError` re-raise, and non-repository exception wrapping.
- [x] 5.3 Test `apply_codecs()` recursive normalization for lists, dicts, `Uri`, `Runnable`, mappings, and non-string sequences.
- [x] 5.4 Test `MiscPyTypeCodec` mapping and sequence behavior, including excluding `str`, `bytes`, and `bytearray`.
- [x] 5.5 Test `NodeCodec` same-index ref reuse, committed cross-DAG import, uncommitted cross-index rejection, and import failure wrapping.

## 6. Live API Integration Tests

- [x] 6.1 Add `tests/integration/test_api_live_runtime_integration.py` with a live initialized `Dml` repo under `tmp_path` and local runtime isolation if needed.
- [x] 6.2 Test `new` / `put` / `commit` / `load` for scalar, list, and dict values through the public API.
- [x] 6.3 Test the distinction between `dag["result"]` named lookup and `dag.result` committed result.
- [x] 6.4 Test cross-DAG `require()` from a committed source DAG into a second committed DAG.
- [x] 6.5 Test live collection helpers for list indexing/slicing, append/conj, dict get/default, assoc, and contains.
- [x] 6.6 Test scoped default usage with `use_default_dml(dml)` and top-level `new()` / `load()`.
- [x] 6.7 Test context-manager error capture against a live repository.

## 7. Marker And Verification

- [x] 7.1 Decide whether live API integration tests need `needs_dml`; if used, register the marker in `pyproject.toml`, otherwise use the existing integration marker policy.
- [x] 7.2 Run targeted API contract tests with `uv run --dev --all-extras pytest tests/contracts/test_api_defaults.py tests/contracts/test_api_dag_contracts.py tests/contracts/test_api_node_contracts.py tests/contracts/test_api_codecs.py`.
- [x] 7.3 Run targeted API integration tests with `uv run --dev --all-extras pytest tests/integration/test_api_live_runtime_integration.py`.
- [x] 7.4 Run the fast-path suite with `uv run --dev --all-extras pytest -m "not slow" .`.
- [x] 7.5 Run formatting/lint verification with `uv run --dev --all-extras ruff check .`.
