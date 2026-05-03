# Commit Model

## Status

specified

## Authority

This document is authoritative for the model semantics and invariants described in this document.
If related docs conflict on this scope, this document is the source of truth.


## Purpose

The commit model defines immutable snapshots, branch pointers, and staging state used to version DAG history.

## Entities

- `Commit`: immutable snapshot with `parents`, `tree`, metadata, and optional DAG ref.
- `Tree`: mapping from DAG names to DAG refs.
- Branch refs: file-backed pointers to commits managed by `HeadOps`.
- Index refs: file-backed mutable staging pointers rooted from commits.

## Flow

1. A branch ref selects the current branch commit.
2. An index ref stages DAG mutations from that base.
3. `commit(...)` writes a new commit.
4. Branch-backed index commits move the addressed branch pointer.
5. Function commits produce detached commits (returned via pointer/publication flow) unless a head is explicitly provided.
6. `Tree` stores named DAG references for that commit.

## Merge/Rebase

- Merge computes tree diffs and rejects conflicting DAG-name edits.
- Rebase reapplies source changes on top of a target ancestry line.

## Invariants

- Commits are immutable once written.
- Branch and index movement is explicit through `HeadOps` pointer updates.
- Commit ancestry fully determines history.

## Scope

This document defines the scope described by its authority and purpose sections.

## Content

See the sections in this document for normative content.

## References

None.
