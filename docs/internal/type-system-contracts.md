# Type System Contracts (`_internal.types`)

## Status

specified

## Authority

This document is authoritative for the internal subsystem contract described in this document.
If related docs conflict on this scope, this document is the source of truth.


## Purpose

Type-system contracts define namespace-bound object families and validation invariants for persisted internal data.

## Namespace Registration

- Runtime deserialization relies on `NAMESPACES` mappings.
- Concrete classes must map to stable namespaces.
- Datum subclasses are registered under `datum-*` namespaces.

## Validation Strategy

- Every persisted object type validates in `__post_init__` via `_validate`.
- Validation covers field types and ref namespace expectations.

## Data Categories

- `Datum` hierarchy: scalar/list/dict/uri/runnable payloads.
- `Node` hierarchy: literal/import/function and argument nodes.
- Graph/history structures: `Dag`, `Tree`, `Commit`, `Head`, `Index`.

## Critical Invariants

- `ListDatum` and `DictDatum` hold datum refs (not arbitrary objects).
- Public `Runnable.target` is a `Uri`.
- Internal `RunnableDatum.target` is a ref to `datum-uri`.
- Graph references (e.g., dag result/error/nodes) must reference valid namespaces.
- `Error` is serializable domain state, not only an in-memory exception.

## Error Contract

`DmlRepoError` is the canonical operational error class for repository-domain failures.

## Scope

This document defines the scope described by its authority and purpose sections.

## Content

See the sections in this document for normative content.

## References

None.
