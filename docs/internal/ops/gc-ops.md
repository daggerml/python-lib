# GcOps (`daggerml._internal.ops.gc`)

## Status

specified

## Authority

This document is authoritative for the architecture contract for this internal ops subsystem (responsibilities, invariants, and non-goals).
If related docs conflict on this scope, this document is the source of truth.


## Purpose

`GcOps` identifies and removes unreachable objects from repository storage.

## Responsibilities

- Enumerate orphan refs (`list_orphans`).
- Delete orphan refs and return deletion stats (`gc`).

## Core Contracts

- Default reachability roots are all `head` and `index` refs.
- Optional explicit roots can be provided.
- Empty root set is allowed and treated as full-orphan sweep.

## Invariants

- Orphan computation is delegated to DB-level graph traversal.
- Deletion is best-effort per orphan ref; failures are logged and GC continues.

## Non-goals

- Semantic cleanup rules for URIs/deletables beyond graph reachability.
- Remote object cleanup.

## Scope

This document defines the scope described by its authority and purpose sections.

## Content

See the sections in this document for normative content.

## References

None.
