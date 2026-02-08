# Internal Module (`daggerml._internal`)

## Status

specified

## Authority

This document is authoritative for `daggerml._internal` subsystem boundaries and responsibilities.
If related docs conflict on this scope, this document is the source of truth.


## Purpose

The internal module defines DaggerML's transactional repository runtime, typed object validation, and subsystem orchestration.


## Scope

This document defines:

- internal module boundaries,
- internal subsystem responsibilities,
- references to authoritative docs for each internal subsystem.


## Subsystems

- Storage and refs: [storage-and-refs.md](storage-and-refs.md)
- Ops facade and domain operations: [ops/README.md](ops/README.md)
- Namespace contracts: [namespace.md](namespace.md)
- Storage model: [storage.md](storage.md)
- Type-system contracts: [type-system-contracts.md](type-system-contracts.md)


## Design Rule

Internal contracts optimize for:

- explicit refs and typed state,
- transactional correctness,
- deterministic failure surfaces (`DmlRepoError`) at subsystem boundaries.

## Content

See the sections in this document for normative content.

## References

None.
