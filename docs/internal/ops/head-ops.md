# HeadOps (`daggerml._internal.ops.head`)

## Status

specified

## Authority

This document is authoritative for the architecture contract for this internal ops subsystem (responsibilities, invariants, and non-goals).
If related docs conflict on this scope, this document is the source of truth.


## Purpose

`HeadOps` provides human-readable branch names (for example, `main`) that point to the latest commit for that line of history, and operations that create, inspect, and delete those named pointers.

## Responsibilities

- List existing heads.
- Create new heads from a commit/head or initialize first commit.
- Delete heads.
- Describe a head and its commit pointer.
- Advance an existing head to a resolved commit ref.

## Core Contracts

- `create(branch_name, from_head=None)`:
  - creates initial commit/tree when `from_head` is `None`,
  - otherwise points new head at source commit.
- `delete(head_ref)` requires namespace `head`.
- `describe(head_ref)` returns stable metadata (`id`, `ref`, `commit`).

## Invariants

- Head value always points to a `commit` ref.
- Duplicate branch names are rejected.
- Invalid source refs/namespaces fail with `DmlRepoError`.
- Head advancement requires an existing head and existing commit.

## Non-goals

- Commit graph editing, merge/rebase logic, or DAG-level behavior.

## Scope

This document defines the scope described by its authority and purpose sections.

## Content

See the sections in this document for normative content.

## References

None.
