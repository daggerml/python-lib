# Storage and Reference Model

## Status

specified

## Authority

This document is authoritative for the internal subsystem contract described in this document.
If related docs conflict on this scope, this document is the source of truth.


## Purpose

The storage and reference model defines typed `Ref` identity, transaction guarantees, raw object access, and local-manifest behavior for internal persistence.


## Storage Layer Contract (`_internal._db`)

The DB layer provides:

- Namespace-aware object storage.
- Transaction boundaries (read-only / read-write).
- Typed `Ref` handles (`namespace:id`).
- Raw object access plus local-manifest load capabilities for object graph transfer.

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

- Local-manifest loads must restore valid references and namespace alignment.
- The local-manifest shape is `{kind, schema, root-ns, root-id, closure}` with `kind = "local-manifest"`.
- In `local-manifest`, `closure` stores raw local object dumps as `{namespace: {id: dump_str}}`.
- Subsystems define local-manifest closure collection rules within their authority boundaries.
- When a local-manifest stops at child DAG refs, those child DAGs remain references in the local dump and are resolved/published through remote DAG-ref handoff rules rather than inline closure expansion.

## Scope

This document defines the scope described by its authority and purpose sections.

## Content

See the sections in this document for normative content.

## References

None.
