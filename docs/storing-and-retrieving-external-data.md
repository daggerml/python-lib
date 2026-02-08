# Storing and Retrieving External Data

## Status

specified

## Authority

This document is authoritative for external-data storage semantics (`Uri`, `Deletable`) and lifecycle rules.
If related docs conflict on this scope, this document is the source of truth.


## Purpose

External data storage lets users reference externally stored payloads with `Uri` while DaggerML tracks cleanup intent with `Deletable` as graph reachability changes.

## Core Objects

- External resource locations are represented as `Uri` datums.
- Deletion-intent records are represented as `Deletable` objects.

## Lifecycle

- Artifacts are referenced by URI from DAG/data objects.
- Garbage collection removes unreachable graph objects.
- URI/deletable coordination ensures lifecycle bookkeeping can be explicit.

## Design Constraint

External payload bytes remain outside repository storage; DaggerML tracks references and lifecycle metadata only.

## Scope

This document defines the scope described by its authority and purpose sections.

## Content

See the sections in this document for normative content.

## References

None.
