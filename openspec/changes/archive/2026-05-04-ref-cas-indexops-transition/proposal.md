## Why

File-backed branch and index refs are no longer part of LMDB transaction atomicity, but `IndexOps` still advances them as if they were transactional state. That leaves live paths where `.dml/refs/**` can move to commits that never committed, and it keeps `HeadOps` coupled to caller-owned transactions in places that should now be file-I/O-only.

## What Changes

- Change `HeadOps` pointer-management requirements so branch and index pointer operations are file-backed CAS/delete operations that do not depend on caller-owned transactions.
- Preserve `create_branch(..., txn=...)` as the only transaction-aware `HeadOps` API, with the requirement that it create bootstrap commit state before writing the branch file.
- Add an `IndexOps` optimistic publication workflow that reads the current index commit through `HeadOps`, builds a new immutable commit in LMDB, closes the transaction, and then publishes it through `HeadOps` compare-and-swap.
- Require affected index mutation paths to retry from the current stored commit after `HeadOps` reports a stale pointer conflict.
- Remove the need for temporary index ref files in builtin and failed-execution helper flows by treating those paths as detached scratch commit construction rather than published mutable indexes.

Example target shape:

```python
base_commit = HeadOps(_db=self._db).get_index_commit(index_id)
while True:
    with self._tx(readonly=False) as txn:
        ctx = txn.get_commit_ctx(base_commit)
        new_commit = derive_next_commit(ctx, txn)
    try:
        HeadOps(_db=self._db).update_index_commit(index_id, base_commit, new_commit)
        break
    except DmlPointerConflictError as err:
        base_commit = err.current_commit
```

## Capabilities

### New Capabilities
- `indexops-optimistic-ref-publication`: Defines how `IndexOps` derives commits from a `HeadOps`-provided base commit, publishes them through post-transaction CAS, and retries on conflicts.

### Modified Capabilities
- `headops-pointer-management`: Narrow `HeadOps` transaction support so pointer lookup/update/delete stays inside `HeadOps` file I/O, while `create_branch` remains the only transaction-aware bootstrap entrypoint.

## Impact

- Affected code: `src/daggerml/_internal/ops/head.py`, `src/daggerml/_internal/ops/index.py`, and any callers that currently pass `txn=` into non-bootstrap `HeadOps` methods.
- Affected behavior: index mutation and commit-finalization flows, index listing and deletion ownership, builtin scratch DAG creation, and pointer-conflict retry semantics.
- Affected contracts: internal `HeadOps` transaction boundaries, index publication sequencing, and stale-pointer retry behavior.
