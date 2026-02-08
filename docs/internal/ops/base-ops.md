# BaseOps (`daggerml._internal.ops.base`)

## Status

specified

## Authority

This document is authoritative for the architecture contract for this internal ops subsystem (responsibilities, invariants, and non-goals).
If related docs conflict on this scope, this document is the source of truth.


## Purpose

`BaseOps` provides shared transaction and storage primitives used by all ops subsystems.

## Responsibilities

- Manage transactional context (`_tx`) with read/write mode.
- Provide typed object serialization/deserialization through `TxnContext.put/get`.
- Expose shared helpers for dump/load, object existence, iteration, and context resolution (`get_ctx`).
- Apply retry policy (`with_retry`) for recoverable DB conditions.

## Core Contracts

- All persisted objects must pass `_validate()` before storage.
- Reference namespace must be known to the DB namespace registry.
- Retrieval decodes through `NAMESPACES[ref.ns()]`.
- Storage errors are surfaced as `DmlRepoError` with subsystem context.

## Transaction Semantics

- Readonly transactions never mutate state.
- Write transactions are atomic at DB transaction boundary.
- `with_retry` handles map growth and environment reopen/retry conditions.

## Invariants

- `TxnContext.get_ctx(head_or_index_ref)` resolves a coherent `{head, commit, tree, dag}` snapshot.
- Internal URI/deletable cleanup logic keeps the two namespaces consistent.

## Non-goals

- Domain-specific policy for DAG execution, branch behavior, cache identity, or remote sync.

## Scope

This document defines the scope described by its authority and purpose sections.

## Content

See the sections in this document for normative content.

## References

None.
