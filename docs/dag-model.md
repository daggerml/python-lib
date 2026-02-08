# DAG Model

## Status

specified

## Authority

This document is authoritative for the model semantics and invariants described in this document.
If related docs conflict on this scope, this document is the source of truth.


## Purpose

The DAG model defines immutable computation graphs, staging evolution, and terminal result/error invariants.

## Core Structure

A DAG contains:

- `nodes`: computation/value nodes,
- `names`: stable name-to-node bindings,
- `result` or `error`: terminal outcome,
- optional function call-input metadata:
  - `Dag.argv` points to the `ArgvNode`,
  - `KwargvNode` is represented in `Dag.nodes`.

## Lifecycle

1. `Index` creation starts from a fresh DAG snapshot.
2. Index operations progress by creating new DAG snapshots with added state (nodes, names, result/error).
3. `commit` records the current DAG snapshot in commit history.
4. Committed DAG snapshots are loaded/referenced by tree and commit state.

Modification semantics:

- DAGs are immutable.
- Index execution never mutates a DAG in place; it advances by creating a new DAG snapshot.
- Once an index is committed, that committed DAG snapshot is final and remains unchanged.

## Identity and Hashing

- Nodes are content-addressed (hashed).
- Adding a node is idempotent by node content; repeated adds resolve to the same node identity.
- `names` is a mapping from strings to node refs, so multiple names may point to the same node.
- DAGs are also content-addressed (hashed).
- Two DAGs with identical structure (same nodes/topology, same names, same terminal state) resolve to the same DAG identity.

## Node Kinds

- `LiteralNode`: wraps datum refs.
- `FnNode`: references a function-result DAG and original call node refs.
- `ImportNode`: references a node from another DAG.
- `ArgvNode` / `KwargvNode`: function-call input metadata.

## Invariants

- A finished DAG has either `result` or `error`.
- Node refs in `names` must be present in `nodes`.
- Function DAG links are explicit through refs, not implicit object pointers.

## Scope

This document defines the scope described by its authority and purpose sections.

## Content

See the sections in this document for normative content.

## References

None.
