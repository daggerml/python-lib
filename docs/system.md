# System Module Layering

## Status

specified

## Authority

This document is authoritative for the system architecture layering and subsystem boundaries.
If related docs conflict on this scope, this document is the source of truth.


## Purpose

System architecture defines the public-to-internal layering and subsystem boundaries.


## Layering

1. Public API (`daggerml.api`): ergonomic user interface.
2. Internal Ops (`_internal.ops`): transactional domain operations.
3. Types (`_internal.types`): object model + validation.
4. Storage (`_internal._db`): LMDB-backed persistence primitives.

## Subsystem Responsibilities

- `HeadOps`: branch pointers.
- `CommitOps`: history, merge, rebase, commit description, and DAG lookup.
- `DagOps`: DAG read/query surface.
- `IndexOps`: mutable staging + execution.
- `NodeOps`: value retrieval/unrolling.
- `CacheOps`: cache interface for cache-key derivation and remote-backed cache-ref operations.
- `RemoteOps`: S3 CAS+refs sync.
- `GcOps`: orphan identification/removal.

## Design Principles

- Explicit refs over implicit object links.
- Transactional atomicity at subsystem boundaries.
- Deterministic validation and error surfacing.

## Scope

This document defines the scope described by its authority and purpose sections.

## Content

See the sections in this document for normative content.

## References

None.
