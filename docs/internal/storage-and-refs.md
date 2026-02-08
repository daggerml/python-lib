# Storage and Reference Model

## Status

specified

## Authority

This document is authoritative for the internal subsystem contract described in this document.
If related docs conflict on this scope, this document is the source of truth.


## Purpose

The storage and reference model defines typed `Ref` identity, transaction guarantees, and dump/load behavior for internal persistence.


## Storage Layer Contract (`_internal._db`)

The DB layer provides:

- Namespace-aware object storage.
- Transaction boundaries (read-only / read-write).
- Typed `Ref` handles (`namespace:id`).
- Raw dump/load capabilities for object graph transfer.

## Reference Semantics

- Every persisted object is addressed by `Ref`.
- Namespace prefixes are part of the type system contract, not just naming.
- Namespace mismatches are treated as contract violations.

## Object Identity

- Identity is reference-based, not Python-object-based.
- Any operation accepting refs must validate namespace expectation before use.

## Transactional Invariants

- All mutations happen in explicit write transactions.
- Readers should observe valid object graphs; partial mutations are not visible across transaction boundaries.
- Failed operations should not leave partially-written graph structures.

## Dump/Load Expectations

- Dumps are opaque serialized blobs that can be loaded back into storage.
- Load operations must restore valid references and namespace alignment.

## Scope

This document defines the scope described by its authority and purpose sections.

## Content

See the sections in this document for normative content.

## References

None.
