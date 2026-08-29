## Context

The current DB boundary exposes `DmlDbEnv`, `DmlDbEnvTxn`, transaction retry helpers, and env lifecycle concerns across `_internal/dml.py` and the ops layer. That split has encouraged constructor-injected hidden DB state, repeated cross-op instantiation with shared `_db`, and raw transaction escapes such as `txn.txn.get(..., raw=True)`. The new design collapses raw DB lifecycle into `_db.pyx`, introduces one typed facade in `types.py`, and makes DB usage explicit at every ops call site.

`types.py` must remain agnostic to project layout. It may know DaggerML object types and namespaces, but it must not derive DB paths from project homes or own repository layout helpers.

## Goals / Non-Goals

**Goals:**
- Make `_db.pyx` the only owner of LMDB env and transaction lifecycle.
- Replace shared env reuse with one env+txn opened together per active DB context manager entry.
- Make `daggerml._internal.types.DmlDB` the only typed DB boundary used by application code.
- Move object validation to `DmlDB.put` and remove redundant per-type validation hooks used only for persistence.
- Remove hidden constructor-owned DB state from ops classes and require explicit `db: DmlDB` arguments instead.
- Replace env reopen logic with fail-fast PID invalidation.
- Preserve map-full retry through `DmlDB.run_with_resize(...)`.

**Non-Goals:**
- Preserve any backward compatibility for old `_db.pyx`, `BaseOps`, or constructor-injected ops APIs.
- Make `types.DmlDB` responsible for project path resolution.
- Keep `RunnableDatum` or any other redundant storage wrapper that only exists to satisfy the old boundary.

## Decisions

### 1. `_db.pyx` exposes a single raw transactional context manager
`_db.pyx` will expose `dmldb(path, *, readonly, create=False, map_size=None, ...)` and a small set of error classes. Entering the context manager opens the env and immediately opens a transaction in that env; exiting commits or aborts and then closes both.

Why:
- Removes shared env reuse from application code.
- Removes the need for env reopen and repair logic.
- Matches the desired safety boundary for concurrent callers.

Alternative considered:
- Keep a reusable env object and only hide it better. Rejected because it preserves the shared-env lifetime that caused the current failure mode.

### 2. `types.DmlDB` is a reusable typed facade, but DB access is only valid inside `with db:`
`types.DmlDB` may be instantiated and re-entered multiple times. Each `__enter__` delegates to `dmldb(...)` and activates one raw transaction object; each `__exit__` closes it. `DmlDB` itself owns typed read/write helpers, not project path lookup.

Why:
- Keeps typed operations centralized.
- Allows callers to reuse one Python object without keeping a live env/txn around.
- Keeps project layout concerns in orchestration code rather than in type definitions.

Alternative considered:
- Add `for_project(...)` on `DmlDB`. Rejected because project layout knowledge does not belong in `types.py`.

### 3. Validation moves from model types to `DmlDB.put`
Stored model classes will stop owning persistence validation as their main guardrail. `DmlDB.put` will validate namespaces, graph shapes, and typed invariants before encoding the object for storage.

Why:
- Puts persistence validation at the persistence boundary.
- Simplifies stored model types.
- Enables removal of redundant wrappers such as `RunnableDatum`.

Alternative considered:
- Keep `_validate` methods on model objects and call them indirectly from `put`. Rejected because it keeps validation ownership fragmented.

### 4. `require` remains a static ref validator, and `get_ctx` becomes the shared typed context loader
`DmlDB.require` will be the migrated form of the current `require_ref` utility. It validates `Ref` shape and namespace expectations; it does not fetch from the DB. `DmlDB.get_ctx` will replace `get_commit_ctx` naming and own the common commit/tree/dag context loading behavior.

Why:
- Keeps lightweight ref validation cheap and side-effect free.
- Provides one short, general context-loading API.

Alternative considered:
- Add `expected_type`-style fetched-object validation to `require`. Rejected because that changes it from a ref validator into a DB read helper.

### 5. Ops become stateless and accept explicit operational inputs
Ops classes will no longer be initialized with `_db`. Methods that need DB access will accept `db: DmlDB` explicitly, and each ops method will take only the concrete non-DB inputs it actually needs for that operation.

`project_home` remains an orchestration concern owned by `Dml` and `HeadOps`. `IndexOps` must not accept or recover `project_home`; callers should resolve branch/index pointer state before invoking it.

Why:
- Removes hidden shared mutable state.
- Makes DB ownership visible at call sites.
- Aligns ops methods with the new transaction boundary.

Alternative considered:
- Keep ops instances stateful but rebuild them per call with a new DB. Rejected because it preserves a misleading object model with little benefit.

### 6. PID changes invalidate the active raw transaction immediately
The raw `_db.pyx` layer will continue checking PID ownership. If the PID changes while a transaction is active, the env/txn is invalidated and the operation fails. No reopen or repair path remains.

Why:
- Simpler than env repair.
- Safer than attempting to recover a forked active transaction.

Alternative considered:
- Reopen after PID mismatch. Rejected explicitly.

## Signature Matrix

### `_internal._db.pyx`

```python
@contextmanager
def dmldb(path: str, *, readonly: bool, create: bool = False, map_size: int | None = None): ...

class RawDmlDB:
    path: str
    readonly: bool
    closed: bool

    def put(self, value, *, ns: str | None = None, to: Ref | None = None, raw: bool = False, no_overwrite: bool = False) -> Ref: ...
    def get(self, ref: Ref, *, raw: bool = False): ...
    def exists(self, ref: Ref) -> bool: ...
    def delete(self, ref: Ref) -> None: ...
    def iter(self, ns: str, start_token: str | None = None): ...
    def list_orphans(self, start_refs: list[Ref]) -> list[Ref]: ...
    def get_size(self) -> int: ...
    def resize(self, new_size: int) -> None: ...
    def commit(self) -> None: ...
    def abort(self) -> None: ...
```

### `daggerml._internal.types.DmlDB`

```python
class DmlDB:
    def __init__(self, path: str, *, readonly: bool = False, create: bool = False, map_size: int | None = None) -> None: ...
    def __enter__(self) -> "DmlDB": ...
    def __exit__(self, exc_type, exc, tb) -> None: ...

    @staticmethod
    def require(ref: Ref, expected_ns: str | list[str]) -> Ref: ...

    @classmethod
    def run_with_resize(
        cls,
        fn,
        *,
        path: str,
        readonly: bool = False,
        create: bool = False,
        initial_map_size: int | None = None,
        max_map_size: int,
    ): ...

    def put(self, obj, *, to: Ref | None = None, ns: str | None = None, no_overwrite: bool = False) -> Ref: ...
    def get(self, ref: Ref): ...
    def exists(self, ref: Ref) -> bool: ...
    def delete(self, ref: Ref) -> None: ...
    def iter(self, ns: str, start_token: str | None = None): ...
    def get_raw(self, ref: Ref) -> str: ...
    def put_raw(self, data: str, *, to: Ref | None = None, ns: str | None = None, no_overwrite: bool = False) -> Ref: ...
    def list_orphans(self, start_refs: list[Ref]) -> list[Ref]: ...
    def get_ctx(self, commit_ref: Ref) -> CommitCtx: ...
```

### Ops classes

Ops classes remain instantiable with no DB constructor arguments:

```python
HeadOps()
DagOps()
NodeOps()
CommitOps()
CacheOps()
RemoteOps(...)
GcOps()
IndexOps()
```

### `HeadOps`

```python
def create_branch(self, project_home: str, branch_name: str, from_commit: Ref | None = None, *, db: DmlDB | None = None) -> str: ...
def delete_branch(self, project_home: str, branch_name: str) -> None: ...
def get_branch_commit(self, project_home: str, branch_name: str | None) -> Ref: ...
def update_branch_commit(self, project_home: str, branch_name: str, old_commit: Ref, new_commit: Ref) -> Ref: ...
def create_index(self, project_home: str, commit_ref: Ref, execution_id: str) -> str: ...
def delete_index(self, project_home: str, execution_id: str) -> None: ...
def get_index_commit(self, project_home: str, execution_id: str) -> Ref: ...
def list_pointer_roots(self, project_home: str) -> list[Ref]: ...
def update_index_commit(self, project_home: str, execution_id: str, old_commit: Ref, new_commit: Ref) -> Ref: ...
def get_head_state(self, project_home: str) -> HeadState: ...
def resolve_head_commit(self, project_home: str) -> Ref: ...
def get_attached_head_branch(self, project_home: str) -> str | None: ...
def require_attached_head_branch(self, project_home: str) -> str: ...
def write_attached_head(self, project_home: str, branch_name: str) -> str: ...
def write_detached_head(self, project_home: str, commit_ref: Ref) -> Ref: ...
def require_commit(self, commit_ref: Ref, *, db: DmlDB) -> None: ...
```

### `DagOps`

```python
def describe(self, dag_ref: Ref, *, db: DmlDB) -> dict: ...
def get_argv(self, dag_ref: Ref, *, db: DmlDB) -> list[Ref] | None: ...
def get_kwargv(self, dag_ref: Ref, *, db: DmlDB) -> dict[str, Ref] | None: ...
def describe_node(self, name: str, *, dag: Ref, db: DmlDB) -> dict: ...
```

### `NodeOps`

```python
def describe(self, node_ref: Ref, *, db: DmlDB) -> dict: ...
def value(self, node_ref: Ref, *, db: DmlDB): ...
def unroll(self, ref: Ref, *, db: DmlDB): ...
def require_node_ref(self, node_ref: Ref, *, db: DmlDB) -> Ref: ...
```

### `CommitOps`

```python
def is_ancestor(self, ancestor: Ref, descendant: Ref, *, db: DmlDB) -> bool: ...
def list(self, head: Ref, *, db: DmlDB, limit: int | None = None): ...
def merge(self, commit1: Ref, commit2: Ref, user: str, *, db: DmlDB) -> Ref: ...
def merge_into_head(self, project_home: str, branch: str, other: Ref, user: str, *, db: DmlDB) -> Ref: ...
def revert(self, project_home: str, branch: str, commit: Ref, user: str, *, db: DmlDB) -> Ref: ...
def checkout_dag(self, project_home: str, branch: str, source_commit: Ref, source_name: str, *, target_name: str | None = None, replace: bool = False, user: str, db: DmlDB) -> Ref: ...
def rebase(self, source: Ref, target: Ref, user: str, *, db: DmlDB) -> Ref: ...
def get_dag(self, commit: Ref, name: str, *, db: DmlDB) -> Ref | None: ...
def describe(self, commit: Ref, *, db: DmlDB) -> dict: ...
def delete_dag(self, project_home: str, name: str, branch: str | None, user: str, *, db: DmlDB) -> None: ...
```

### `CacheOps`

```python
def get_cache_key(self, argv_ref: Ref, *, db: DmlDB) -> str: ...
def get(self, argv_ref: Ref, *, remote_root: str, db: DmlDB) -> Ref | None: ...
def put(self, dag_ref: Ref, *, execution_id: str, remote_root: str, db: DmlDB) -> str: ...
```

### `RemoteOps`

```python
def __init__(self, *, bucket: str, prefix: str, fetch_workers: int = ..., client=None) -> None: ...
def put_ref_manifest(self, root_ref: Ref, *, db: DmlDB) -> str: ...
def load_ptr(self, manifest_oid: str, *, db: DmlDB, expected_root_ns: str | None = None) -> Ref: ...
def load_ptr_in_txn(self, manifest_oid: str, *, db: DmlDB, expected_root_ns: str | None = None) -> Ref: ...
```

### `GcOps`

```python
def list_orphans(self, *, db: DmlDB, project_home: str) -> list[Ref]: ...
def collect(self, *, db: DmlDB, project_home: str) -> dict: ...
```

### `IndexOps`

```python
def create(self, execution_id: str, *, db: DmlDB, base_commit: Ref | None = None, argv: Ref | None = None) -> Ref: ...
def get_node(self, commit_ref: Ref, name: str, *, db: DmlDB) -> Ref: ...
def get_argv(self, commit_ref: Ref, *, db: DmlDB) -> Ref: ...
def get_kwargv(self, commit_ref: Ref, *, db: DmlDB) -> Ref: ...
def put_literal(self, commit_ref: Ref, value, *, db: DmlDB, name: str | None = None) -> tuple[Ref, Ref]: ...
def put_import(self, commit_ref: Ref, dag: Ref, *, db: DmlDB, node: Ref | None = None, name: str | None = None) -> tuple[Ref, Ref]: ...
def set_node_name(self, commit_ref: Ref, name: str, node: Ref, *, db: DmlDB) -> tuple[Ref, Ref]: ...
def start_fn(self, commit_ref: Ref, argv: list[Ref], *, db: DmlDB, remote_root: str, kwargv: dict[str, Ref] | None = None, name: str | None = None) -> tuple[Ref | None, Ref | None]: ...
def commit(self, commit_ref: Ref, value, *, db: DmlDB, message: str | None = None, dag_name: str | None = None) -> Ref: ...
def describe(self, commit_ref: Ref, *, db: DmlDB) -> dict: ...
def build_cancel_plan(self, execution_id: str, requested_by: str, max_workers: int, *, db: DmlDB, remote_root: str) -> dict[str, list[str]]: ...
```

## File Migration Matrix

- `_internal/_db.pyx`: replace env/txn API with `dmldb(...)` and raw transaction object.
- `_internal/types.py`: add `DmlDB`, move persistence validation to `put`, add `require`, `get_ctx`, raw passthroughs, and `run_with_resize`.
- `_internal/ops/base_ops.py`: delete.
- `_internal/ops/head.py`: remove `_db` constructor dependency and use explicit `project_home`/`db` parameters.
- `_internal/ops/dag.py`: convert all DB-using methods to explicit `db: DmlDB`.
- `_internal/ops/node.py`: convert all DB-using methods to explicit `db: DmlDB`.
- `_internal/ops/commit.py`: convert all DB-using methods to explicit `db: DmlDB` and explicit `project_home` where needed.
- `_internal/ops/cache.py`: convert to explicit `db: DmlDB` and `remote_root` parameters.
- `_internal/ops/remote.py`: replace raw `txn.txn.*` escapes with `DmlDB` methods and pass `db` explicitly.
- `_internal/ops/gc.py`: replace raw orphan-listing escape with `DmlDB.list_orphans`.
- `_internal/ops/index.py`: convert all DB-using methods to explicit `db: DmlDB` plus only the concrete refs/values required for each operation; do not pass `project_home` into `IndexOps`.
- `_internal/dml.py`: stop constructing ops with `_db`; create explicit `DmlDB` contexts, resolve pointer state through `HeadOps`, and call `IndexOps` with explicit refs/values.

## Risks / Trade-offs

- More explicit `db` plumbing across ops methods -> Mitigation: make this a deliberate, greenfield signature rewrite and migrate bottom-up.
- Opening env+txn per context-manager entry may cost more than env reuse -> Mitigation: prefer correctness first and benchmark after the boundary is simplified.
- Moving validation to `DmlDB.put` can temporarily create gaps if any invariant is missed -> Mitigation: convert validations systematically and add focused DB-boundary tests.
- Raw helpers needed by remote and GC flows can leak low-level details back upward -> Mitigation: constrain them to named `DmlDB` methods such as `get_raw`, `put_raw`, and `list_orphans`.
