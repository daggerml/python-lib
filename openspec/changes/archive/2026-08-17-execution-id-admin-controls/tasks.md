## 1. Execution-State Operations

- [x] 1.1 Add cache description that reads one cache-pointer snapshot, describes that exact execution without DAG materialization, returns reusable terminal identity only for unmarked records, and safely handles absent, dangling, and rebound pointers.
- [x] 1.2 Replace cache-key invalidation roots with execution-ID roots and implement root selection, propagated-caller pointer eligibility, branch pruning, direct caller-edge traversal, cacheless roots, deduplication, locking, conditional pointer deletion, and immutable marking.
- [x] 1.3 Update invalidation response typing so selected cacheless roots report `cache_key: str | None`.

## 2. Public API And CLI

- [x] 2.1 Add `CacheDescription` and expose `Dml.cache.describe(cache_key: str) -> CacheDescription | None` with `execution: Ref`, `dag: Ref | None`, and lifecycle fields.
- [x] 2.2 Change `Dml.cache.invalidate(*executions: Ref)` to validate runtime refs, reject strings and wrong namespaces, require at least one execution, and delegate exact execution IDs.
- [x] 2.3 Rename `Dml.runtime.cancel(index=...)` to `Dml.runtime.cancel(execution=...)` without changing cancellation behavior.
- [x] 2.4 Verify the generated CLI exposes `cache describe CACHE_KEY`, parses execution refs for `cache invalidate`, and serializes descriptions and invalidation responses as JSON.

## 3. Contract Coverage

- [x] 3.1 Add execution-state contracts for cache description across absent, dangling, running, reusable terminal, marked terminal, and concurrent pointer-rebound cases.
- [x] 3.2 Add invalidation contracts for explicit rebound roots, current callers, rebound historical caller pruning, no replacement selection, no ancestor traversal above pruned callers, cacheless roots, missing records, and duplicate roots.
- [x] 3.3 Update public surface and CLI contracts for exact signatures, metadata, valid refs, empty inputs, wrong namespaces, string rejection, command help, parsing, and serialization.

## 4. Documentation And Verification

- [x] 4.1 Update Python, CLI, cache, runtime, refresh, and execution architecture documentation to describe cache inspection and execution-ref administrative controls.
- [x] 4.2 Run `uv run --dev --all-extras pyright`, `uv run --dev --all-extras ruff check --fix .`, `uv run --dev --all-extras pytest -m "not slow" .`, and `git diff --check`.
