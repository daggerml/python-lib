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
3. compute direct DAG ids for the pushed commit from that commit's `Tree.dags`,
4. ensure each directly referenced DAG has a `refs/dags/<dag_id>.json` entry, recursing only when a missing direct DAG itself directly references other missing DAGs,
5. upload missing CAS objects after hash verification,
6. upload/verify manifest object for the closure,
7. write destination ref path and payload according to [remote-data-model.md](remote-data-model.md).

Rules:

- push publication MUST target only the tag-ref namespace/keying defined in [remote-data-model.md](remote-data-model.md).
- push publication MUST resolve the source commit and write a tag ref pointing to the manifest for that commit closure.
- push publication MUST derive direct DAG ids only from the pushed commit's `Tree.dags`, not from the transitive dumped closure.
- DAG publication on miss MUST derive direct child DAG ids only from that DAG's own nodes.
- non-commit, non-dag manifest publication MUST derive direct DAG ids from the root-owned object graph without traversing into child DAG roots.
- local closure collection for remote publication MUST stop at child DAG refs; child DAG contents MUST NOT be embedded into the parent local-manifest closure.
- manifest `closure["dag"]` and ref `targets["dag"]` written during push MUST contain only direct DAG ids for that manifest layer.
- tag/cache ref publication MUST validate that `targets["dag"]` exactly equals the referenced manifest's `closure["dag"]`; on mismatch, publication MUST fail and no ref may be written.
- before writing any ref that points at a manifest (`refs/tags/**`, `refs/cache/**`, or `refs/dags/**`), the publisher MUST ensure the manifest CAS bytes exist remotely and that the manifest OID matches the SHA-256 of those canonical bytes.
- per-DAG publication fast path MUST check only whether `refs/dags/<dag_id>.json` exists; it MUST NOT verify the target CAS on that fast path.
- if `refs/dags/<dag_id>.json` already exists, publication MUST treat that DAG as already published and MUST NOT inspect descendants of that DAG.
- if a direct DAG ref is missing, publication MUST inspect only that missing DAG's direct child DAG ids and recurse only for those missing direct children.
- push to an existing tag-ref path MUST fail deterministically (no in-place overwrite).
- project branch push MAY update `refs/projects/<owner>/<project>/heads/<branch>.json` only by conditional ETag match.
- project branch push MUST reject non-fast-forward updates unless force is requested; force still requires the ETag condition.
- project branch creation MUST use create-if-absent semantics and fail if another writer creates the ref first.
- project tag publication under `refs/projects/<owner>/<project>/tags/<tag>.json` MUST be immutable.
- destination ref paths MUST satisfy segment/path constraints defined in [remote-data-model.md](remote-data-model.md).
- push sync operations MUST NOT write transport blobs under `io/**`.
- concurrent DAG-ref creation races MUST be resolved by handling `RefAlreadyExists`, reading back the existing ref, and accepting that as the canonical result.


## Pull Protocol

Pull operation MUST:

1. read ref JSON from the requested ref path in the ref layout defined by [remote-data-model.md](remote-data-model.md),
2. read and validate target manifest per [remote-data-model.md](remote-data-model.md),
3. resolve any `closure["dag"]` entries via `refs/dags/<dag_id>.json` and recurse into those child manifests,
4. fetch missing CAS objects referenced by manifest closure,
5. verify fetched CAS object hashes,
6. materialize fetched state into local storage,
7. update local pulled-head pointer for the resolved commit.

Rules:

- pull MUST fail when required remote objects are missing or invalid.
- pull MUST fail when manifest/root contracts are invalid.
- pull MUST fail when a manifest references a DAG id whose `refs/dags/<dag_id>.json` entry is missing.
- pull/load MUST reject tag/cache refs that point at manifests but omit `targets`.
- pull/load MUST use one centralized internal recursive materialization path.
- pull/load MUST deduplicate recursive DAG manifest loads within one top-level materialization.
- pull/load MUST stop scheduling a manifest or CAS object once it is already present locally or already scheduled within the active materialization.
- pull/load local materialization MAY recursively load child DAG manifests only through `closure["dag"]` resolution via `refs/dags/**`; it MUST NOT infer child DAG closure from raw DAG CAS presence alone.
- project fetch MUST materialize the addressed branch or tag and update a local tracking head named by canonical `dml://<owner>/<project>#<branch>` or `dml://<owner>/<project>@<tag>`.
- pull/load MAY fetch independent manifest refs and CAS objects concurrently.
- pull sync operations MUST NOT require transport blobs under `io/**`.


## Cache Ref Operations

Remote cache refs are managed in the cache-ref namespace defined by [remote-data-model.md](remote-data-model.md).

This section defines protocol-level behavior only.
Cache API surface/method signatures are defined by cache-facing subsystem docs.

Rules:

- `<cache_key>` MUST use execution cache-key identity defined in [adapter-execution-contract.md](adapter-execution-contract.md).
- `<cache>` namespace constraints are defined in [remote-data-model.md](remote-data-model.md).
- protocol implementations MUST support cache-ref enumeration over `refs/cache/`.
- protocol implementations MUST support cache-ref read/write/delete by cache key.
- cache-ref writes MUST create when absent.
- cache-ref writes MUST fail deterministically when the cache-key path already exists.
- cache ref writes MUST be deterministic for a given cache key.
- cache-ref writes MUST include top-level `targets` for the direct DAG ids of the referenced manifest.
- reruns that want to publish the same cache key MUST invalidate or delete the current cache ref before a later write can succeed.


## Prune/GC Protocol

Rules:

- cache refs are manually managed and MUST NOT be deleted by age-based policy.
- `prune()` MUST NOT delete cache-ref namespace entries by expiry metadata.
- remote GC MUST use mark-and-sweep with roots from remaining refs (`tags`, `cache`).
- remote GC MUST keep all manifest targets and non-`dag` closure OIDs reachable from those roots.
- remote GC MUST resolve `closure["dag"]` through `refs/dags/**`; `refs/dags/**` are not GC roots by themselves.
- remote GC MUST support malformed-object handling modes `raise`, `warn`, and `ignore`; default behavior is `warn`.
- `malformed="raise"` MUST fail immediately with a clear error naming the malformed object and why it is malformed.
- `malformed="warn"` MUST emit a clear warning naming the malformed object and why it is malformed, then delete the malformed object if present and continue.
- `malformed="ignore"` MUST continue silently but still delete malformed objects if present.
- missing `refs/dags/**` entries are not malformed for GC purposes and MUST be skipped as unreachable.
- prune/gc semantics in this document apply to CAS+refs only.
- `io/invoke/**` blobs are ephemeral transport data and MAY be deleted by age-based cleanup.
- transport cleanup MUST be independent of CAS/ref reachability (no mark-and-sweep roots for `io/invoke/**`).
- missing `io/invoke/**` objects at execution time MUST fail deterministically; caller/executor restaging is required for retry.


## Integrity and Failure Rules

- hash mismatch is a hard failure.
- invalid descriptor/ref/manifest shape is a hard failure at decode/validation boundaries.
- malformed remote data MUST never be silently accepted.
- operations MUST fail closed on validation errors.
- GC MAY continue past malformed remote data only under an explicit malformed-object policy that permits continuation.
- if a `refs/dags/**` entry exists but its target manifest CAS object is missing, that is a broken remote state; publication fast paths do not repair it, and consumers that need the manifest MUST fail.


## References

- [remote-data-model.md](remote-data-model.md)
- [remote-sync.md](remote-sync.md)
- [adapter-execution-contract.md](adapter-execution-contract.md)
