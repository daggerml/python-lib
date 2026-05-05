# DmlOps (`daggerml._internal.ops.dml`)

## Status

specified

## Authority

This document is authoritative for the `DmlOps` facade contract.

## Purpose

`DmlOps` is the internal runtime facade that owns repository lifecycle and access to ops subsystems.

## Scope

This document defines the `DmlOps` facade surface and subsystem accessor contracts. It does not define commit, DAG, index, cache, or remote operation semantics.

## Responsibilities

- construct typed ops modules against one shared `DmlDbEnv`,
- enforce that subsystem access only happens while the DB is open,
- provide a single root for internal callers: `commit()`, `head()`, `index()`, `dag()`, `node()`, `cache()`, `gc()`, `remote(...)`,
- expose runtime configuration naming compatible with [../../configuration.md](../../configuration.md),
- require `remote_root` for all `DmlOps` instances,
- expose `cache()` with cache context derived from `remote_root`,
- expose `remote(client=None)` with remote context derived from `remote_root`.

## Invariants

- `self._db is None` means subsystem access is invalid and raises `RuntimeError`.
- every ops instance created by `DmlOps` references the same DB handle.
- `create(...)` initializes the default branch and attaches `.dml/HEAD` through `HeadOps`.
- `remote_root` is always a configured remote-root string when a `DmlOps` instance exists.
- `cache()` forwards remote-root context without redefining cache semantics.
- `remote(...)` forwards remote context without redefining remote semantics.
- mutable project workflows default their destination branch from attached `.dml/HEAD` when the caller does not pass an explicit branch.
- runtime callers are responsible for mapping config key `remote.root` onto `DmlOps.remote_root` consistently with [../../configuration.md](../../configuration.md).

## Non-goals

- business logic for commits, DAGs, execution, cache, remote, or GC,
- cross-subsystem transaction orchestration beyond providing shared DB context,
- defining remote schema or cache behavior.

## References

- [../../default-dml-runtime.md](../../default-dml-runtime.md)
- [../../configuration.md](../../configuration.md)
- [../../remote-data-model.md](../../remote-data-model.md)
- [../../remote-protocol.md](../../remote-protocol.md)
- [../../remote-sync.md](../../remote-sync.md)
- [remote-ops.md](remote-ops.md)
