# Ops Module (`daggerml._internal.ops`)

## Status

specified

## Authority

This document is authoritative for `daggerml._internal.ops` module boundaries, layering, and operation ownership.
If related docs conflict on this scope, this document is the source of truth.


## Purpose

The ops module defines repository-domain operations over shared transactional storage.


## Layering

- `DmlOps`: repository/session facade and subsystem factory.
- `BaseOps`/`TxnContext`: shared transaction and object IO mechanics.
- Specialized ops modules: `head`, `commit`, `index`, `dag`, `node`, `cache`, `gc`, `remote`.


## Submodule Docs

- `DmlOps`: [dml-ops.md](dml-ops.md)
- `BaseOps`: [base-ops.md](base-ops.md)
- `HeadOps`: [head-ops.md](head-ops.md)
- `CommitOps`: [commit-ops.md](commit-ops.md)
- `DagOps`: [dag-ops.md](dag-ops.md)
- `IndexOps`: [index-ops.md](index-ops.md)
- `NodeOps`: [node-ops.md](node-ops.md)
- `CacheOps`: [cache-ops.md](cache-ops.md)
- `GcOps`: [gc-ops.md](gc-ops.md)
- `RemoteOps`: [remote-ops.md](remote-ops.md)


## Invariants Across Ops

- Inputs are ref-validated at subsystem boundaries.
- Each subsystem assumes transactional consistency from `BaseOps`.
- Ops boundaries expose deterministic repository errors (`DmlRepoError`).

## Scope

This document defines the scope described by its authority and purpose sections.

## Content

See the sections in this document for normative content.

## References

None.
