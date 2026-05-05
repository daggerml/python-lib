# HeadOps (`daggerml._internal.ops.head`)

## Status

specified

## Authority

This document is authoritative for the architecture contract for this internal ops subsystem (responsibilities, invariants, and non-goals).
If related docs conflict on this scope, this document is the source of truth.


## Purpose

`HeadOps` provides human-readable branch names (for example, `main`) that point to the latest commit for that line of history, and operations that create, inspect, and delete those named pointers.

## Responsibilities

- List existing local branches.
- Create new branch pointers from a commit or initialize the first commit.
- Delete branch pointers.
- Resolve branch, tag-tracking, and index pointers to commit refs.
- Advance existing branch and index pointers with stale-write protection.

## Core Contracts

- `create_branch(branch_name, from_commit=None)`:
  - creates the initial commit/tree when `from_commit` is `None`,
  - otherwise points the branch at the provided commit.
- Local branch, tag, and index pointers are stored as files under `.dml/refs/local/**`.
- Remote-tracking branch and tag pointers are stored as files under `.dml/refs/remote/<owner>/<project>/**`.

## Invariants

- Pointer payloads resolve to `commit` refs.
- Duplicate branch names are rejected.
- Invalid source refs or identifiers fail with `DmlRepoError`.
- Branch and index advancement requires an existing pointer and the expected current commit for mutation paths.
- Non-bootstrap `HeadOps` pointer methods do not validate that referenced commits currently exist in LMDB.

## Non-goals

- Commit graph editing, merge/rebase logic, or DAG-level behavior.

## Scope

This document defines the scope described by its authority and purpose sections.

## Content

See the sections in this document for normative content.

## References

None.
