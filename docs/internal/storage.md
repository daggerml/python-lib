# Storage Model

## Status

specified

## Authority

This document is authoritative for storage-layer concepts, identity model, and persistence semantics.
If related docs conflict on this scope, this document is the source of truth.


## Purpose

The storage model defines how repository objects are persisted, addressed, and retained.

## Backing Store

- LMDB-backed object store via `_internal._db`.
- Objects are encoded by namespace and object id.

## Identity

- Every object is referenced as `Ref("namespace:id")`.
- Namespaces partition object categories (`dag`, `node-*`, `datum-*`, etc.).

## Persistence Rules

- Objects validate before write.
- Reads decode through namespace-to-type mapping.
- Dumps/loads serialize object graphs for cross-process execution boundaries.

## Reachability

- GC reachability roots are typically `head` and `index` refs.
- Unreachable objects can be removed by GC operations.

## Scope

This document defines the scope described by its authority and purpose sections.

## Content

See the sections in this document for normative content.

## References

None.
