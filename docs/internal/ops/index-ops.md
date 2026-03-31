---
status: specified
doc_type: spec
---

# IndexOps (`daggerml._internal.ops.index`)

## Authority

This document is authoritative for `IndexOps` responsibilities and internal operation contracts for mutable staging, function execution orchestration, literal staging, import staging, and commit finalization.

## Scope

This doc covers index creation, literal/import staging, function-execution orchestration, codec call sites, and index commit finalization behavior.

## Purpose

Define `IndexOps` behavior boundaries and invariants for staging-time execution paths.

## Glossary

- IndexOps: The orchestrator for staging changes to the DAG. Defined normatively in this document.
- ArgvNode: Remote DAG node representing positional arguments. Out of authority, see remote data model.
- KwargvNode: Remote DAG node representing keyword arguments. Out of authority, see remote data model.
- ImportNode: Remote DAG node representing imported references. Out of authority, see remote data model.
- DmlRepoError: Base exception type for repository-level errors. Defined normatively in this document's error semantics.
- DAG: Directed Acyclic Graph representing the data model. Out of authority, see remote data model.

## Contract

### Interfaces

- `create(head=...)`
  - Starts from commit/tree lineage.
- `create(argv_ptr=...)`
  - Resolves remote argv payload and stages `ArgvNode` + derived `KwargvNode`.
- `put_literal(...)`
  - Owns literal normalization and storage staging.
  - Must call `daggerml._internal.codec.apply_codec(...)` on its values.
- `start_fn(...)`
  - Orchestrates function execution.
  - Must call `daggerml._internal.codec.apply_codec(...)` on `argv` and `kwargv` values.
  - First call arg resolves to runnable.
  - Kwargs resolve inner-most to outer-most by key.
  - Unknown key raises `DmlRepoError("Unknown kwarg: <key>")`.
  - Builtin execution precedes adapter invocation.
  - Cache-hit resolution precedes adapter invocation for non-builtin calls.
  - Non-builtin execution always performs remote cache lookup by argv identity before adapter invocation.
  - On cache miss, `start_fn(...)` MUST stage adapter inputs in one local transaction, invoke the adapter outside that transaction, then resolve the cached result in a later transaction.
  - Adapter invocation payload MUST publish `argv_ptr` through the remote manifest path before subprocess execution.
  - Adapter `pending` and `running` status MUST return `None` without materializing a `FnNode`.
  - Adapter `succeeded` status requires cache-backed result resolution.
- `put_import(...)`
  - Stages `ImportNode` refs from committed DAG nodes.
- `commit(head=...)`
  - Writes new commit and deletes index ref.
  - If `head=` is provided, updates that head.
  - If `head=` is omitted, commit is detached and returned.

Unspecified interface fields are rejected.

### Invariants

- Index methods validate index ownership and DAG membership.
- Function result DAG contains `result` or `error`.
- Successful function-result materialization creates an `FnNode` that links the original call-site node refs to the resolved function DAG.
- Commit finalization writes commit state and removes index ref.

### Error Semantics

- `DmlRepoError("Unknown kwarg: <key>")`
  - Terminal, non-retryable.
  - Caller must provide valid keyword arguments matching the function signature.
- Missing cache entry on adapter `succeeded` status
  - Terminal, non-retryable, deterministic failure.
  - Requires inspection of the adapter execution output and caching mechanism.

### Authority Handoffs

- `daggerml._internal.codec.apply_codec(...)` behavior is out of scope; hands off to the Codec System spec.
- Adapter execution details and `succeeded` status cache-backed result resolution are out of scope; hands off to the Adapter Execution Contract spec.
- DAG node structures (`ArgvNode`, `KwargvNode`, `ImportNode`) are out of scope; hands off to the Remote Data Model spec.

## Compatibility

- These internal contracts dictate staging DAG structural integrity and must guarantee backward compatibility for repository reads across minor versions. Version migrations, if required, must ensure existing staged indexes remain valid or are deterministically invalidated.

## References

- [../../codec-system.md](../../codec-system.md)
- [../../adapter-execution-contract.md](../../adapter-execution-contract.md)
- [../../execution-model.md](../../execution-model.md)
- [../../remote-data-model.md](../../remote-data-model.md)
