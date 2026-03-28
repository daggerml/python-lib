# Remote Sync

## Status

specified

## Authority

This document is authoritative for the remote-sync concept boundary:

- goals and scope of remote synchronization,
- high-level model and lifecycle,
- non-goals and execution-boundary framing,
- separation between remote data model and remote operation protocol.

If this document conflicts with operation-level details, [remote-protocol.md](remote-protocol.md) is authoritative for operation specifics.
If this document conflicts with remote data shape/layout details, [remote-data-model.md](remote-data-model.md) is authoritative.

## Purpose

Remote sync architecture defines the high-level synchronization lifecycle between local repositories and remote CAS+refs storage.

Detailed data-at-rest contracts are specified in [remote-data-model.md](remote-data-model.md).
Detailed operation protocol is specified in [remote-protocol.md](remote-protocol.md).

## Scope

This document defines conceptual sync lifecycle and boundaries only.
This document does not define remote object schemas, ref layouts, cache subsystem interface semantics, or operation sequencing details.

## Content

## Model

Remote synchronization uses a content-addressed object layer plus discoverable references.
This separates immutable data transport from mutable naming/discovery concerns.

Conceptually:

- content is synchronized by object identity,
- references control what remote state is visible/discoverable,
- DAG publication/discovery uses per-DAG refs under `refs/dags/**`,
- pull materializes referenced state locally.

## Operations

- Push publishes local state to remote and updates discoverable refs.
- Push publishes direct DAG dependencies layer-by-layer rather than flattening all transitive DAGs into one top-level publication step.
- Pull resolves remote refs and materializes required state locally.

## Integrity

- Sync is integrity-first: malformed data and hash mismatches are hard failures.
- Remote state is only accepted through validated protocol boundaries.

## References

- [remote-data-model.md](remote-data-model.md)
- [remote-protocol.md](remote-protocol.md)
