# DagOps (`daggerml._internal.ops.dag`)

## Status

specified

## Authority

This document is authoritative for the architecture contract for this internal ops subsystem (responsibilities, invariants, and non-goals).
If related docs conflict on this scope, this document is the source of truth.


## Purpose

`DagOps` provides read/query access to stored DAG objects and their call-input nodes.

## Responsibilities

- List and describe DAG metadata.
- Resolve named nodes from finished DAGs.
- Return DAG call-input nodes (`argv`, `kwargv`) by ref.

## Core Contracts

- `get_node(dag_ref, name)` requires:
  - valid `dag` ref,
  - DAG exists,
  - DAG is finished (`result` or `error` present),
  - named node exists.
- `get_argv(dag_ref)` returns `dag.argv`.
- `get_kwargv(dag_ref)` finds the unique `KwargvNode` in `dag.nodes` and returns its ref.
  - if none exists: error,
  - if more than one exists: error.

## Invariants

- Returned refs are stable pointers into persisted DAG state.

## Non-goals

- Function execution, node materialization, cache lookups, or commit/head mutation.

## Scope

This document defines the scope described by its authority and purpose sections.

## Content

See the sections in this document for normative content.

## References

None.
