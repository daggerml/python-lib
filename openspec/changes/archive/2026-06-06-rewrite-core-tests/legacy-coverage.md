## Legacy `_core` Coverage Mapping

- `tests/_core/test_head.py` -> `tests/contracts/test_core_head_refs.py` for ref validation, local/remote ref round-trips, HEAD modes, and path-safe names.
- `tests/_core/test_uri.py` -> `tests/contracts/test_core_head_refs.py` and `tests/contracts/test_core_revision_selectors.py` for generated project URI and selector contracts.
- `tests/_core/test_serde.py` -> `tests/contracts/test_core_serde_values.py` for supported DML serde round-trips and malformed envelope rejection.
- `tests/_core/test_types.py` and `tests/_core/test_db.py` -> `tests/contracts/test_core_types_contracts.py` for namespace/object validation and typed facade invariants used by raw transaction wrappers.
- `tests/_core/test_config.py` -> `tests/contracts/test_core_config_resolution.py` for precedence, flattening, coercion, and remote validation contracts.
- `tests/_core/test_dml.py`, `tests/_core/test_index.py`, and `tests/_core/test_commit.py` -> `tests/integration/test_core_parallel_*_integration.py` for DB-backed init, runtime create, same-index mutation, branch commit merge, status/log/runtime-list reads, and coherent repository state.
- `tests/_core/test_exec_state.py` and relevant fake-S3 portions of `tests/_core/test_s3_cas.py` -> `tests/contracts/test_core_execution_coordination.py` for deterministic CAS, lock, same-cache-key coordination, and spawned execution record update contracts.

Trivial fixed parser examples and broad implementation-delegation checks were dropped rather than preserved one-for-one because the new generated and contract-focused tests cover the meaningful behavior.
