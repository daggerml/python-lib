# Namespaces

## Status

specified

## Authority

This document is authoritative for reference namespace semantics and namespace-level validation expectations.
If related docs conflict on this scope, this document is the source of truth.


## Purpose

The namespace model defines reference namespaces and the validity rules for cross-layer reference usage.

## Ref Format

- Canonical format: `namespace:id`.
- `namespace` encodes object family.
- `id` is stable object identity in that namespace.

## Common Namespaces

- DAG/versioning: `dag`, `commit`, `tree`, `head`, `index`
- Node types: `node-literal`, `node-fn`, `node-import`, `node-argv`, `node-kwargv`
- Datum types: `datum-scalar`, `datum-list`, `datum-dict`, `datum-uri`, `datum-runnable`
- Error/lifecycle: `error`, `deletable`

## Rules

- Namespace must match object type on read/write.
- Cross-namespace misuse is a validation/runtime error.
- Namespace checks are part of type validation and ops boundary checks.
- Execution cache keys (for example `<argv-id>`) are key-format values, not persisted object namespaces.

## Scope

This document defines the scope described by its authority and purpose sections.

## Content

See the sections in this document for normative content.

## References

None.
