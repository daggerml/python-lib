# CacheOps (`daggerml._internal.ops.cache`)

## Status

specified

## Authority

This document is authoritative for the architecture contract for this internal ops subsystem (responsibilities, invariants, and non-goals).
If related docs conflict on this scope, this document is the source of truth.


## Purpose

`CacheOps` is the cache interface used by execution paths and callers for cache-keyed result lookup/publication.

## Responsibilities

- Derive cache keys from `node-argv` refs.
- Expose cache-ref operations (`list|get|put|delete|clear`) through one cache-facing subsystem interface.
- Use configured remote context (`remote.root`, `remote.cache`) to resolve cache-ref operations via `RemoteOps`.

## Core Contracts

- Cache key format follows [../../adapter-execution-contract.md](../../adapter-execution-contract.md).
- `_cache_ref(argv_ref)` requires namespace `node-argv`.
- Cache identity semantics are defined in [../../adapter-execution-contract.md](../../adapter-execution-contract.md).
- Remote cache-ref storage/layout is defined in [../../remote-data-model.md](../../remote-data-model.md).
- Cache-ref operation semantics (conflict/idempotence/overwrite behavior) are defined in [../../remote-protocol.md](../../remote-protocol.md).
- `CacheOps` MUST NOT persist function-result cache entries in LMDB cache namespaces.
- Cache operations that require remote context MUST fail deterministically when required context is unavailable.
- `CacheOps.put(dag_ref)` derives canonical cache identity from `dag.argv`; caller-provided helper keys are not authoritative for cache identity.

## Invariants

- DAGs without `argv` are not cacheable.
- Cache identity determinism requirements are defined in [../../adapter-execution-contract.md](../../adapter-execution-contract.md).
- determinism of `argv_ref.id()` for equivalent call payloads MUST be covered by unit tests.
- Cache-ref CRUD through `CacheOps` is remote-backed and uses cache namespace constraints from [../../remote-data-model.md](../../remote-data-model.md).
- cache publication is idempotent for same target and conflict semantics are defined by [../../remote-protocol.md](../../remote-protocol.md).

## Non-goals

- Redefining remote protocol behavior for cache refs.
- Computing cache key from runnable payload directly (handled by execution path).

## Scope

This document defines the scope described by its authority and purpose sections.

## Content

See the sections in this document for normative content.

## References

None.
