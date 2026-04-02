---
status: specified
doc_type: spec
---

# DaggerML Spec Overview

## Authority

This document is authoritative for the concept-to-authority index of the DaggerML spec suite.
It defines which document is the normative source for each high-level concept named here.
It MUST NOT be used to define subsystem behavior, API semantics, or documentation layout rules that belong to another authoritative document.

## Scope

In scope:

- mapping high-level concepts to their authoritative documents;
- stating whether authority for a concept is held by one document or a fixed document set;
- defining caller behavior when a concept is not mapped here.

Out of scope:

- subsystem behavior, data models, runtime behavior, and user-facing API or CLI contracts;
- repository documentation layout and general reader onboarding;
- edit pre-read requirements and path-based documentation workflow.

## Purpose

This spec exists so concept ownership stays explicit and stable across the documentation set.

## Glossary

- **API**: term owned by `docs/api.md` for public API behavior referenced by this mapping index.
- **Authority Mapping**: a normative association between a named concept and the document or document set that owns that concept.
- **argv**: term owned by `docs/adapter-execution-contract.md` for execution argument vectors referenced by this mapping index.
- **CLI**: term owned by `docs/cli.md` for command-line behavior referenced by this mapping index.
- **Concept**: a high-level subject area whose normative definition is owned by an authoritative document.
- **Contrib**: term owned by the mapped `docs/contrib/*.md` authority documents for contrib module behavior.
- **DAG**: term owned by `docs/dag-model.md` for directed acyclic graph semantics referenced by this mapping index.
- **DaggerML**: repository and product name used by this mapping index.
- **Dag.cache()**: term owned by `docs/api.md` and `docs/internal/ops/cache-ops.md` for cache-publication behavior.
- **Spec Suite**: the set of DaggerML documents that define normative behavior and contracts.
- **Document Set**: a fixed set of documents that together own one concept when a single document is insufficient.
- **GC**: term owned by `docs/internal/storage.md` and `docs/internal/storage-and-refs.md` for storage cleanup behavior.
- **JSON**: term owned by `docs/contrib/s3-store.md` for JSON helper behavior referenced by this mapping index.
- **node-wrapper**: term owned by `docs/api.md` and `docs/object-model.md` for public node wrapper behavior.
- **S3**: term owned by `docs/contrib/s3-store.md` for S3-backed contrib storage behavior.
- **S3Store**: term owned by `docs/contrib/s3-store.md` for the contrib S3 storage helper.
- **URI**: term owned by `docs/storing-and-retrieving-external-data.md` and `docs/contrib/s3-store.md` for external resource identifiers.

## Contract

### Interfaces

This document exposes one interface: the Authority Mapping table below.

Each mapping entry consists of:

- `concept`: the canonical concept name used by this document;
- `authority`: one document path or a fixed Document Set of document paths;
- `scope`: the portion of the concept owned by that authority.

Mapping entries MUST use repository-relative document paths.
Mapping entries MUST NOT include fields other than `concept`, `authority`, and `scope`.
Mapping entries with unspecified fields are rejected.
When a concept is not listed in this document, callers MUST treat it as unresolved rather than inferring authority from proximity or naming.

| concept | authority | scope |
| --- | --- | --- |
| Public API behavior | `docs/api.md`, `docs/object-model.md`, `docs/errors.md` | Public Python API semantics, node-wrapper selection, DAG-call staging behavior, and user-visible API errors. |
| CLI behavior | `docs/cli.md` | User-visible CLI commands, arguments, and CLI semantics. |
| Execution and runtime behavior | `docs/configuration.md`, `docs/execution-model.md`, `docs/adapter-execution-contract.md` | Runtime configuration, execution flow, and adapter execution contracts. |
| Cache publication and cache identity | `docs/api.md`, `docs/internal/ops/cache-ops.md`, `docs/adapter-execution-contract.md` | `Dag.cache()` behavior, argv-derived cache identity, and adapter-facing cache contract details. |
| Storage and object persistence | `docs/internal/storage.md`, `docs/internal/storage-and-refs.md`, `docs/storing-and-retrieving-external-data.md` | Storage model, reference handling, GC-adjacent storage behavior, and external data persistence semantics. |
| Commit and DAG semantics | `docs/commit-model.md`, `docs/dag-model.md`, `docs/internal/ops/commit-ops.md`, `docs/internal/ops/dag-ops.md` | Commit objects, DAG model semantics, and operation-layer commit/DAG behavior. |
| Remote sync and protocol | `docs/remote-sync.md`, `docs/remote-data-model.md`, `docs/remote-protocol.md`, `docs/internal/ops/remote-ops.md` | Remote lifecycle, remote schemas, sync protocol semantics, and remote operations behavior. |
| Codec encoding and import/export behavior | `docs/codec-system.md` | Codec registry behavior, encoding rules, and import/export semantics. |
| Contrib API surface | `docs/contrib/api.md` | `daggerml.contrib.api` decorators, delayed actions, and execution helpers. |
| Contrib literal codecs and dataframe serialization | `docs/contrib/codecs.md` | Contrib-owned codec behavior and dataframe serialization semantics. |
| Contrib prebuilt funks | `docs/contrib/funks.md` | Contrib-owned prebuilt function contracts. |
| Contrib testing helpers | `docs/contrib/testing.md` | Testing helpers intended for author-code unit tests. |
| Contrib runtime lifecycle | `docs/contrib/runtime-contract.md`, `docs/contrib/executor-state.md`, `docs/contrib/executor-catalog.md`, `docs/contrib/execution-graph.md` | Supervisor launch, executor start/poll/gc, shared state and comms propagation, adapter/executor pairing, live execution-graph storage, and cancel/sweep runtime behavior. |
| Contrib plugin packaging and discovery | `docs/contrib/registries.md` | Adapter and executor registry contracts, plugin packaging, and discovery behavior. |
| Contrib runtime diagnostics and status surfaces | `docs/contrib/status.md`, `docs/cli.md` | Contrib runtime status and diagnostics APIs plus CLI pass-through behavior under `dml contrib`. |
| Contrib S3 utility behavior | `docs/contrib/s3-store.md` | `S3Store`, S3 URI normalization, content-addressed S3 object helpers, JSON helpers, tar helpers, and extraction safety rules. |

### Invariants

- Every Concept listed in this document MUST map to exactly one authoritative document path or one fixed Document Set.
- This document MUST NOT add behavioral requirements beyond identifying concept ownership.
- If two concepts have different authorities, the mapping MUST keep them as separate entries rather than merging ownership into one rule.
- When a concept changes in a way that changes ownership or adds a new authoritative document, this document MUST be updated in the same change.
- A mapped authority document MUST remain the final normative source for that concept if this document and the mapped document differ.

### Error Semantics

- **Unresolved concept**: non-retryable, terminal for this interface. Callers MUST treat the concept as unresolved and MUST NOT infer authority without an explicit mapping in this document.
- **Stale mapping**: non-retryable for readers and terminal until docs are updated. Callers MUST treat the mapping as invalid and MAY only surface the failure; they MUST NOT redirect to a guessed replacement path. When a mapped path no longer matches the owning concept, maintainers MUST update this document in the same change that moved or split authority.
- **Conflicting authority claim**: non-retryable and terminal. Callers MUST treat the concept as unresolved until the conflict is removed and MUST NOT choose one claimant by precedence. If another document claims ownership of a concept mapped here, the conflict MUST be resolved by updating this document and the conflicting authority text together.

### Authority Handoffs

- Documentation layout, module entry points, and general navigation are handed off to `docs/README.md`.
- System layering and subsystem boundaries are handed off to `docs/system.md`.
- Path-based edit pre-read requirements are handed off to `docs/DOC_MAP.md`.

## Compatibility

Adding a new Concept entry is backward compatible.
Refining the `scope` text of an existing mapping is backward compatible if authority ownership does not change.
Renaming a Concept, removing a Concept entry, changing the authority path for an existing Concept, or changing a single-document authority into a different authority or Document Set is a breaking change to this interface and MUST be coordinated with updates to the affected authoritative documents.
Breaking changes to this interface MUST be signaled by updating this document in the same repository change as the affected authority documents; this repository provides no separate version number for the overview interface.
This document provides no forward-compatibility guarantee for unmapped concepts.

## References

- `docs/README.md`
- `docs/system.md`
- `docs/DOC_MAP.md`
