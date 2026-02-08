# Remote Protocol

## Status

specified

## Authority

This document is authoritative for remote sync operation behavior:

- push/pull protocol steps,
- cache ref operation semantics,
- remote prune/gc operation behavior,
- operation-level integrity and failure rules.

If remote docs conflict on data shape/layout/schemas, [remote-data-model.md](remote-data-model.md) is authoritative for those specifics.


## Purpose

The remote protocol defines how sync operations execute against remote CAS+refs storage.


## Scope

This document defines operation semantics only.
This document does not redefine remote layout or object schema contracts.


## Contract References

- Remote data-at-rest schema/layout: [remote-data-model.md](remote-data-model.md)
- Remote sync lifecycle framing: [remote-sync.md](remote-sync.md)
- Execution cache-key identity: [adapter-execution-contract.md](adapter-execution-contract.md)


## Content

Sync operations in this document operate against the protocol root defined in [remote-data-model.md](remote-data-model.md) (`<remote.root>/dml/` when project-root config is used).

## Push Protocol

Push operation MUST:

1. validate remote descriptor/layout compatibility per [remote-data-model.md](remote-data-model.md),
2. resolve the local commit root and closure for publication,
3. upload missing CAS objects after hash verification,
4. upload/verify manifest object for the closure,
5. write destination ref path and payload according to [remote-data-model.md](remote-data-model.md).

Rules:

- push publication MUST target only the tag-ref namespace/keying defined in [remote-data-model.md](remote-data-model.md).
- push publication MUST resolve the source commit and write a tag ref pointing to the manifest for that commit closure.
- push to an existing tag-ref path MUST fail deterministically (no in-place overwrite).
- destination ref paths MUST satisfy segment/path constraints defined in [remote-data-model.md](remote-data-model.md).
- push sync operations MUST NOT write transport blobs under `io/**`.


## Pull Protocol

Pull operation MUST:

1. read ref JSON from the requested ref path in the ref layout defined by [remote-data-model.md](remote-data-model.md),
2. read and validate target manifest per [remote-data-model.md](remote-data-model.md),
3. fetch missing CAS objects referenced by manifest closure,
4. verify fetched CAS object hashes,
5. materialize fetched state into local storage,
6. update local pulled-head pointer for the resolved commit.

Rules:

- pull MUST fail when required remote objects are missing or invalid.
- pull MUST fail when manifest/root contracts are invalid.
- pull sync operations MUST NOT require transport blobs under `io/**`.


## Cache Ref Operations

Remote cache refs are managed in the cache-ref namespace defined by [remote-data-model.md](remote-data-model.md).

This section defines protocol-level behavior only.
Cache API surface/method signatures are defined by cache-facing subsystem docs.

Rules:

- `<cache_key>` MUST use execution cache-key identity defined in [adapter-execution-contract.md](adapter-execution-contract.md).
- `<cache>` namespace constraints are defined in [remote-data-model.md](remote-data-model.md).
- protocol implementations MUST support cache-ref enumeration per cache namespace.
- protocol implementations MUST support cache-ref read/write/delete by cache namespace/key.
- cache-ref writes MUST create when absent.
- cache-ref writes MUST be no-op success when existing `target` matches.
- cache-ref writes MUST fail deterministically on target conflict unless explicit overwrite behavior is requested by caller.
- protocol implementations MAY support explicit overwrite behavior for cache-ref writes.
- cache ref writes MUST be deterministic for a given cache key.


## Prune/GC Protocol

Rules:

- cache refs are manually managed and MUST NOT be deleted by age-based policy.
- `prune()` MUST NOT delete cache-ref namespace entries by expiry metadata.
- remote GC MUST use mark-and-sweep with roots from remaining refs (`tags`, `cache`).
- remote GC MUST keep all manifest targets and closure OIDs reachable from those roots.
- prune/gc semantics in this document apply to CAS+refs only.
- `io/invoke/**` blobs are ephemeral transport data and MAY be deleted by age-based cleanup.
- transport cleanup MUST be independent of CAS/ref reachability (no mark-and-sweep roots for `io/invoke/**`).
- missing `io/invoke/**` objects at execution time MUST fail deterministically; caller/executor restaging is required for retry.


## Integrity and Failure Rules

- hash mismatch is a hard failure.
- invalid descriptor/ref/manifest shape is a hard failure at decode/validation boundaries.
- malformed remote data MUST never be silently accepted.
- operations MUST fail closed on validation errors.


## References

- [remote-data-model.md](remote-data-model.md)
- [remote-sync.md](remote-sync.md)
- [adapter-execution-contract.md](adapter-execution-contract.md)
