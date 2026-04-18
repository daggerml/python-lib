# CacheOps (`daggerml._internal.ops.cache`)

## Status

specified

## Authority

This document is authoritative for the `CacheOps` subsystem contract.

## Purpose

`CacheOps` is the cache interface used by execution paths and callers for cache-keyed result lookup and publication.

## Responsibilities

- derive cache keys from `node-argv` refs,
- expose cache-ref operations `list|get|put|delete|clear`,
- use configured remote context (`remote.root`) to resolve cache-ref operations through `RemoteOps`.

## Core Contracts

- cache key format follows [../../adapter-execution-contract.md](../../adapter-execution-contract.md),
- `_cache_key(argv_ref)` requires namespace `node-argv` and returns the underlying argv datum-list id,
- cache identity semantics are defined in [../../adapter-execution-contract.md](../../adapter-execution-contract.md),
- remote cache-ref storage layout is defined in [../../remote-data-model.md](../../remote-data-model.md),
- cache-ref operation semantics are defined in [../../remote-protocol.md](../../remote-protocol.md),
- `CacheOps` MUST NOT persist function-result cache entries in LMDB cache namespaces,
- cache operations that require remote context MUST fail deterministically when that context is unavailable,
- `CacheOps.put(dag_ref)` derives canonical cache identity from `dag.argv`,
- `CacheOps.put(dag_ref)` MUST publish the referenced DAG manifest through `RemoteOps.put_ref_manifest(...)`,
- `CacheOps.list()` yields `(cache_key, dag_ref)` pairs.

## Invariants

- DAGs without `argv` are not cacheable,
- cache publication is remote-backed and uses `refs/cache/<cache_key>.json`,
- cache publication is idempotent for the same target subject to remote protocol rules,
- cache keys are strings derived from `argv_ref.id()` and are not LMDB refs.

## Non-goals

- redefining remote protocol behavior for cache refs,
- computing cache keys from runnable payloads directly.

## References

- [../../adapter-execution-contract.md](../../adapter-execution-contract.md)
- [../../remote-data-model.md](../../remote-data-model.md)
- [../../remote-protocol.md](../../remote-protocol.md)
