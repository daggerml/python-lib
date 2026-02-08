# DmlOps (`daggerml._internal.ops.dml`)

## Status

specified

## Authority

This document is authoritative for the architecture contract for this internal ops subsystem (responsibilities, invariants, and non-goals).
If related docs conflict on this scope, this document is the source of truth.


## Purpose

`DmlOps` is the internal runtime facade that owns repository lifecycle and access to all ops subsystems.

## Scope

This document defines the `DmlOps` facade surface and subsystem accessor contracts.
This document does not define commit/DAG/index/cache/remote operation behavior.

## Content

## Responsibilities

- Construct typed ops modules against one shared `DmlDbEnv`.
- Enforce that subsystem access only happens while DB is open.
- Provide a single root for internal callers (`commit()`, `head()`, `index()`, `dag()`, `node()`, `cache()`, `gc()`, `remote(...)`).
- Expose and preserve runtime configuration naming compatible with [../../configuration.md](../../configuration.md).
- Expose cache accessor with no required cache-context arguments:
  - `cache()`.
- `cache()` MUST derive cache context from `DmlOps` runtime attributes:
  - `remote_root`: remote project-root URI context.
  - `remote_cache`: remote cache namespace context.
- Expose remote accessor with no required remote-context arguments:
  - `remote(client=None)`.
- `remote()` MUST derive remote context from `DmlOps` runtime attributes:
  - `remote_root`: remote project-root URI context.

## Invariants

- `self._db is None` means subsystem access is invalid and raises `RuntimeError`.
- Every ops instance created by `DmlOps` references the same DB handle.
- `create(...)` initializes the default head (`head:main`) through `HeadOps`.
- `cache()` forwards remote cache context without redefining cache operation semantics.
- `remote(...)` forwards remote context without redefining remote operation semantics.
- runtime callers are responsible for mapping config keys (`remote.root`, `remote.cache`) onto `DmlOps` attrs (`remote_root`, `remote_cache`) consistently with [../../configuration.md](../../configuration.md).

## Non-goals

- Business logic for commits, DAGs, execution, cache, remote, or GC.
- Cross-subsystem transaction orchestration beyond providing shared DB context.
- Defining remote schema/layout constraints or cache operation semantics.

## References

- [../../default-dml-runtime.md](../../default-dml-runtime.md)
- [../../configuration.md](../../configuration.md)
- [remote-data-model.md](../../remote-data-model.md)
- [remote-protocol.md](../../remote-protocol.md)
- [remote-sync.md](../../remote-sync.md)
- [remote-ops.md](remote-ops.md)
