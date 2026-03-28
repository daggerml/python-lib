# Remote Data Model

## Status

specified

## Authority

This document is authoritative for remote data-at-rest contracts:

- remote CAS+refs layout and path namespaces,
- remote transport-blob layout and path namespaces,
- remote descriptor schema,
- manifest/ref object schemas and invariants,
- manifest OID identity shape,
- cache-ref path and namespace constraints.

If remote docs conflict on these items, this document is the source of truth.


## Purpose

The remote data model defines what data exists in remote storage and what invariants that data must satisfy.


## Scope

This document defines remote object shape and storage layout only.
This document does not define push/pull sequencing, cache ref operations, or prune/gc operation behavior.


## Content

## Definitions

- OID: lowercase 64-char SHA-256 hex of canonical object bytes.
- CAS: immutable object storage keyed by OID.
- Manifest: CAS object that defines a root object and its materialization closure.
- Ref: JSON object under `refs/` that targets a manifest OID.
- Manifest OID: manifest CAS object identity passed directly between runtime components without a `refs/...` wrapper object.
- Project root: `s3://<bucket>/<project-prefix>/` location provided by runtime config (`remote.root`).
- Protocol root: `<project-root>/dml/` location containing DML remote protocol data.


## Remote Layout

Project root: `s3://<bucket>/<project-prefix>/`

```text
<project-prefix>/
  dml/
    dml.json

    refs/
      tags/
        <name>/<version>.json
      cache/
        <cache>/
          <cache_key>.json
      dags/
        <dag_id>.json

    io/
      invoke/
        <invoke_id>.json

    cas/
      sha256/
        <aa>/<bb>/<oid>
```

Rules:

- DML remote protocol data MUST live under protocol root `<project-root>/dml/`.
- `<aa>` and `<bb>` are the first 2 + next 2 hex chars of OID.
- OIDs and manifest/ref targets MUST be strict lowercase 64-char hex.
- cache refs are single-key entries per cache namespace (`refs/cache/<cache>/<cache_key>.json`).
- transport blobs under `io/**` are adapter-transport payload objects, not refs and not CAS objects.

## Ref Namespace Roles

- `refs/tags/**`: named publication paths for user-facing branch/tag style discovery.
- `refs/cache/**`: mutable cache-key pointers for function-result memoization.
- `refs/dags/**`: per-DAG indirection entries that map a logical DAG id to that DAG's manifest OID.

## Transport Namespace Roles

- `io/invoke/**`: reloadable transport blobs for adapter/executor boundaries.

Rules:

- `io/invoke/<invoke_id>.json` object content MAY be any JSON-serializable transport payload required by runtime boundaries.
- transport payloads representing adapter invocation input MUST match adapter stdin payload shape defined in [adapter-execution-contract.md](adapter-execution-contract.md).
- `<invoke_id>` MUST match `[a-z0-9][a-z0-9._-]{0,127}`.
- transport blobs are implementation-managed and are outside tag/cache ref mutability rules.

## Ref Path Segment Constraints

Rules:

- ref path segments MUST be non-empty.
- ref path segments MUST NOT be `.` or `..`.
- ref path segments MUST NOT contain `/` or `\\`.
- tag `<name>` MUST match `[a-z0-9][a-z0-9._-]{0,127}`.
- tag `<version>` MUST match `[a-z0-9][a-z0-9._-]{0,127}`.
- dag `<dag_id>` MUST be a lowercase 64-char SHA-256 hex string.


## Descriptor Schema (`dml.json`)

Schema v0:

```json
{
  "schema": 0,
  "hash": "sha256",
  "layout": "cas+refs",
  "refs_prefix": "refs",
  "io_prefix": "io",
  "cas_prefix": "cas/sha256"
}
```

Rules:

- descriptor `schema` MUST be `0`.
- descriptor `hash` MUST be `sha256`.
- descriptor `layout` MUST be `cas+refs`.
- descriptor `refs_prefix` MUST be `refs`.
- descriptor `io_prefix` MUST be `io`.
- descriptor `cas_prefix` MUST be `cas/sha256`.


## Manifest Schema

Schema v0:

```json
{
  "kind": "manifest",
  "schema": 0,
  "root-ns": "commit",
  "root-id": "<oid>",
  "closure": {
    "<ns>": ["<oid>", "<oid>"]
  }
}
```

Invariants:

- `kind == "manifest"` and `schema == 0`.
- `root-ns` and `root-id` are required.
- `closure` is a map of namespace -> sorted, unique OID list.
- `closure["dag"]`, when present, is a sorted unique list of direct logical DAG ids, not manifest OIDs.
- for `commit` roots, `closure["dag"]` is exactly the direct DAG ids from that commit's `Tree.dags` map.
- for `dag` roots, `closure["dag"]` is exactly the direct child DAG ids referenced by that DAG's own nodes.
- reachability for non-DAG objects is defined by the union of all non-`dag` OIDs in `closure`; `closure["dag"]` is resolved through `refs/dags/**`.

Canonicalization and identity:

- manifest canonical bytes MUST be `json.dumps(manifest, separators=(",", ":"), sort_keys=True).encode("utf-8")`.
- manifest OID MUST be the SHA-256 hex digest of those canonical bytes.
- logically equivalent manifests with different JSON key ordering or whitespace are invalid representations for identity purposes.


## Ref Schema

Schema v0:

```json
{
  "kind": "ref",
  "schema": 0,
  "target": "<manifest-oid>",
  "created_at": 1760000000,
  "targets": {"dag": ["<dag-id>"]},
  "meta": {}
}
```

Invariants:

- `kind == "ref"` and `schema == 0`.
- `target` is a manifest OID.
- refs that point at manifests in `refs/tags/**` and `refs/cache/**` MUST include top-level `targets`.
- `targets` currently supports only the `dag` namespace.
- `targets["dag"]` MUST be a sorted unique list of direct logical DAG ids for the referenced manifest.
- tag/cache refs that omit `targets` are malformed.
- `refs/tags/**` entries are write-once per path and MUST NOT be overwritten in place.
- `refs/tags/**` entries MAY be deleted by explicit ref-delete operations; delete+recreate is permitted but discouraged.
- `refs/cache/**` entries are mutable.
- `refs/dags/**` entries are write-once per DAG id path and map a logical DAG id to that DAG manifest OID.
- `refs/dags/**` ref payloads SHOULD set `meta = {"dag": {"id": "<dag_id>"}}`.
- consumers MAY assert that `refs/dags/<dag_id>.json` agrees with `meta.dag.id` when metadata is present.


## Cache Namespace Constraints

Rules:

- `<cache>` MUST be explicit and caller-provided.
- `<cache>` MUST be lowercase ASCII matching `[a-z0-9][a-z0-9._-]{0,127}`.
- `<cache_key>` identity is defined by execution contracts in [adapter-execution-contract.md](adapter-execution-contract.md).


## References

- [remote-sync.md](remote-sync.md)
- [remote-protocol.md](remote-protocol.md)
- [adapter-execution-contract.md](adapter-execution-contract.md)
