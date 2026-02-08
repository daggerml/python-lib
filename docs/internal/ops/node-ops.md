# NodeOps (`daggerml._internal.ops.node`)

## Status

specified

## Authority

This document is authoritative for the architecture contract for this internal ops subsystem (responsibilities, invariants, and non-goals).
If related docs conflict on this scope, this document is the source of truth.


## Purpose

`NodeOps` resolves stored node values for inspection APIs and execution helper paths.

## Responsibilities

- Validate node refs.
- Retrieve shallow node values (`get`).
- Fully unroll nested datum graphs into Python values (`unroll`).
- Describe node metadata (`describe`).

## Core Contracts

- `get(node_ref)` preserves nested refs in collections where appropriate.
- `unroll(node_ref)` recursively resolves datum refs to concrete values.
- Runnable unrolling reconstructs public `Runnable(target, sub, kwargs, adapter)`.

## Invariants

- Cycle detection is enforced while unrolling datum refs.
- Error refs cannot be unrolled as values.
- Unsupported datum types fail with explicit `DmlRepoError`.

## Non-goals

- DAG mutation, function invocation, caching, or commit/head updates.

## Scope

This document defines the scope described by its authority and purpose sections.

## Content

See the sections in this document for normative content.

## References

None.
