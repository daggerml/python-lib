# Remote Data Model

## Status

specified

## Authority

This document is authoritative for remote data-at-rest contracts:

- remote CAS and refs layout,
- remote transport-blob layout,
- remote descriptor schema,
- manifest and ref schemas,
- cache-ref path constraints.

## Purpose

Define what data exists in remote storage and what invariants that data must satisfy.

## Scope

This document defines remote object shape and storage layout only.

## Definitions

- OID: lowercase 64-char SHA-256 hex of canonical object bytes.
- CAS: immutable object storage keyed by OID.
- Manifest: CAS object that defines a root object and its materialization closure.
- Ref: JSON object under `refs/` that targets a manifest OID.
- Project root: `s3://<bucket>/<project-prefix>/` provided by `remote.root`.
- Protocol root: `<project-root>/dml/`.

## Remote Layout

Project root: `s3://<bucket>/<project-prefix>/`

```text
<project-prefix>/
  dml/
    dml.json

    refs/
      projects/
        <owner>/<project>/heads/<branch>.json
        <owner>/<project>/tags/<tag>.json
      tags/
        <name>/<version>.json
      cache/
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

- DML remote protocol data MUST live under `<project-root>/dml/`.
- OIDs and manifest-ref targets MUST be strict lowercase 64-char hex.
- cache refs live at `refs/cache/<cache_key>.json`.
- project branch refs live at `refs/projects/<owner>/<project>/heads/<branch>.json` and are mutable.
- project tag refs live at `refs/projects/<owner>/<project>/tags/<tag>.json` and are write-once.
- legacy `refs/cache/<name>/<cache_key>.json` paths are invalid.
- transport blobs under `io/**` are adapter-transport payload objects, not refs and not CAS objects.

## Ref Namespace Roles

- `refs/tags/**`: named publication paths for branch and tag style discovery.
- `refs/projects/**`: project-scoped branch heads and tags for git-like remote operations.
- `refs/cache/**`: create-only cache-key refs for function-result memoization until explicit invalidation or deletion.
- `refs/dags/**`: per-DAG indirection entries mapping logical DAG ids to DAG manifest OIDs.

## Transport Namespace Roles

- `io/invoke/**`: reloadable transport blobs for adapter and executor boundaries.

Rules:

- `io/invoke/<invoke_id>.json` content MAY be any JSON-serializable transport payload required by runtime boundaries.
- transport payloads representing adapter invocation input MUST match [adapter-execution-contract.md](adapter-execution-contract.md).
- `<invoke_id>` MUST match `[a-z0-9][a-z0-9._-]{0,127}`.

## Ref Path Segment Constraints

Rules:

- ref path segments MUST be non-empty.
- ref path segments MUST NOT be `.` or `..`.
- ref path segments MUST NOT contain `/` or `\\`.
- tag `<name>` and `<version>` MUST match `[a-z0-9][a-z0-9._-]{0,127}`.
- project `<owner>` and `<project>` MUST match `[a-z0-9][a-z0-9._-]{0,127}`.
- project branch and tag names MAY contain slash-separated segments; every segment MUST match `[a-z0-9][a-z0-9._-]{0,127}`.
- dag `<dag_id>` MUST be a lowercase 64-char SHA-256 hex string.

## Descriptor Schema (`dml.json`)

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

- `schema` MUST be `0`.
- `hash` MUST be `sha256`.
- `layout` MUST be `cas+refs`.
- `refs_prefix` MUST be `refs`.
- `io_prefix` MUST be `io`.
- `cas_prefix` MUST be `cas/sha256`.

## Manifest Schema

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
- `closure` is a map of namespace to sorted unique OID lists.
- `closure["dag"]`, when present, is a sorted unique list of direct logical DAG ids, not manifest OIDs.
- manifest canonical bytes are `json.dumps(..., separators=(",", ":"), sort_keys=True).encode("utf-8")`.

## Ref Schema

```json
{
  "kind": "ref",
  "schema": 0,
  "target": "<manifest-oid>",
  "execution_id": "<execution-id>",
  "created_at": 1760000000,
  "targets": {"dag": ["<dag-id>"]},
  "meta": {}
}
```

Invariants:

- `kind == "ref"` and `schema == 0`.
- `target` is a manifest OID.
- refs in `refs/cache/**` MUST include a non-empty `execution_id` string naming the execution that published the cache entry.
- refs in `refs/tags/**` and `refs/cache/**` MUST include `targets`.
- refs in `refs/projects/**` MUST include `targets` and MUST point at commit manifests.
- `targets["dag"]` MUST be a sorted unique list of direct logical DAG ids for the referenced manifest.
- `refs/tags/**` entries are write-once per path.
- `refs/cache/**` entries are create-only and require explicit deletion before republishing the same cache key path.
- `refs/projects/**/heads/**` entries are mutable only through conditional update operations.
- `refs/projects/**/tags/**` entries are write-once per path.
- `refs/dags/**` entries are write-once per DAG id path.

## Cache Ref Constraints

Rules:

- `<cache_key>` identity is defined by [adapter-execution-contract.md](adapter-execution-contract.md).
- cache refs MUST use exactly one filename segment under `refs/cache/`.

## References

- [remote-sync.md](remote-sync.md)
- [remote-protocol.md](remote-protocol.md)
- [adapter-execution-contract.md](adapter-execution-contract.md)
