# CommitOps (`daggerml._internal.ops.commit`)

## Status

specified

## Authority

This document is authoritative for the architecture contract for this internal ops subsystem (responsibilities, invariants, and non-goals).
If related docs conflict on this scope, this document is the source of truth.


## Purpose

`CommitOps` manages commit-history topology and tree-state evolution.

## Responsibilities

- Traverse commit history from a commit ref.
- Compute merge base and diffs between trees.
- Merge commits with DAG-name conflict detection.
- Merge commits into a branch head, fast-forwarding when possible.
- Revert a commit by applying the inverse tree diff as a new branch commit.
- Resolve commit-ish syntax including heads, fetched DML URI tracking heads, and first-parent `~N` walks.
- Checkout one DAG from a resolved commit into the current branch tree.
- Rebase commit ancestry onto a target.
- Describe commits and resolve DAGs from commit tree state.

## Core Contracts

- Commit objects are immutable snapshots; tree references define DAG namespace state.
- Merge/rebase operations produce new commit objects (no in-place history mutation).
- Tree patching applies explicit add/remove DAG-name deltas.

## Invariants

- Parent links define commit DAG topology.
- Merge detects conflicting DAG-name edits and raises `DmlRepoError`.
- User-facing merge/revert/checkout operations advance heads only after writing the resulting commit.
- Rebase preserves source changes while changing ancestry root.

## Non-goals

- Branch head updates (`HeadOps` owns pointer movement).
- Working-index mutation (`IndexOps` owns mutable staging).

## Scope

This document defines the scope described by its authority and purpose sections.

## Content

See the sections in this document for normative content.

## References

None.
