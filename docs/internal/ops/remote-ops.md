# RemoteOps (`daggerml._internal.ops.remote`)

## Status

specified

## Authority

This document is authoritative for the architecture contract for this internal ops subsystem (responsibilities, invariants, and non-goals).
If related docs conflict on this scope, this document is the source of truth.


## Purpose

`RemoteOps` synchronizes repository objects and refs with S3 using the CAS+refs protocol layout.

## Responsibilities

- Ensure remote descriptor (`dml.json`) and layout compatibility.
- Push local objects/refs to remote.
- Pull remote objects/refs into local store.
- Maintain remote cache refs.
- Publish and resolve per-DAG refs under `refs/dags/**`.
- Build `local-manifest` transport payloads for publication handoff.
- Validate content integrity (SHA-256).
- Apply malformed-object policy during remote GC.

## Storage Model

- Remote storage layout and descriptor schema are defined in [../../remote-data-model.md](../../remote-data-model.md).

## Core Contracts

- RemoteOps MUST implement remote operation behavior exactly as specified in [../../remote-protocol.md](../../remote-protocol.md).
- Object/ref validation rules and path/layout contracts are defined in [../../remote-data-model.md](../../remote-data-model.md).
- Push/pull and cache ref operation semantics are defined in [../../remote-protocol.md](../../remote-protocol.md).
- Cache ref namespace/layout constraints are defined in [../../remote-data-model.md](../../remote-data-model.md).
- DAG ref namespace/layout constraints are defined in [../../remote-data-model.md](../../remote-data-model.md).
- Cache conflict/idempotence/overwrite behavior is defined in [../../remote-protocol.md](../../remote-protocol.md).
- Remote prune/cache-retention behavior is defined in [../../remote-protocol.md](../../remote-protocol.md).

## Invariants

- CAS content is addressed by hash and immutable.
- Ref updates are explicit and scoped to requested operations.
- Remote operations fail closed on malformed data.
- Publication handoff payloads use `local-manifest` with raw dump closure entries; remote protocol storage does not.
- Local closure collection for remote publication is direct-only across DAG boundaries: child DAG refs are recorded as direct DAG ids, not expanded inline into the parent closure.
- Tag/cache manifest refs always carry direct-DAG `targets` metadata.
- DAG publication and manifest `closure["dag"]` are direct-only per manifest layer.
- Recursive DAG publication is limited to the explicit publish-on-miss path for missing direct child DAG refs.
- Remote read/materialization uses one centralized recursive materialization path.
- Remote read/materialization stops scheduling a manifest or CAS object once it is already present locally or already scheduled within the active materialization.
- Remote read/materialization may fetch independent manifest refs and CAS objects concurrently.

## Non-goals

- Multi-remote policy orchestration.
- Conflict resolution semantics beyond defined push/pull behavior.

## Scope

This document defines the scope described by its authority and purpose sections.

## Content

See the sections in this document for normative content.

## References

None.
