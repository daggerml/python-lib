# Object Model

## Status

specified

## Authority

This document is authoritative for the model semantics and invariants described in this document.
If related docs conflict on this scope, this document is the source of truth.


## Purpose

The object model defines DaggerML's core datum, node, and versioning object families and the invariants that relate them.

## Fundamental Types

- `Ref`: namespace-qualified identity (`namespace:id`).
- Datums:
  - `ScalarDatum`,
  - `ListDatum`,
  - `DictDatum`,
  - `Uri`,
  - `RunnableDatum`.
- Graph nodes:
  - `LiteralNode`, `FnNode`, `ImportNode`, `ArgvNode`, `KwargvNode`.
- Versioning objects:
  - `Dag`, `Commit`, `Tree`, `Head`, `Index`.

## Runnable Types

- Public `Runnable`: `target` (`Uri`), `sub`, `kwargs`, `adapter`.
- Internal `RunnableDatum`: stored ref form of the same shape.

## Validation Rule

Every persisted object validates its type/namespace constraints before storage.

## Error Boundary

Subsystem boundaries surface repository-domain failures as `DmlRepoError`.

## Scope

This document defines the scope described by its authority and purpose sections.

## Content

See the sections in this document for normative content.

## References

None.
