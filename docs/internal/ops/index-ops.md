---
status: specified
doc_type: spec
---

# IndexOps (`daggerml._internal.ops.index`)

## Authority

This document is authoritative for `IndexOps` responsibilities and internal operation contracts for mutable staging, function execution orchestration, literal staging, import staging, and commit finalization.

## Purpose

Define `IndexOps` behavior boundaries and invariants for staging-time execution paths.

## Scope

This doc covers index creation, literal/import staging, function-execution orchestration, codec call sites, and index commit finalization behavior.

## Content

- `create(head=...)` starts from commit/tree lineage.
- `create(argv_ptr=...)` resolves remote argv payload and stages `ArgvNode` + derived `KwargvNode`.
- `put_literal(...)` owns literal normalization + storage staging.
- `IndexOps` MUST call `daggerml._internal.codec.apply_codec(...)` on:
  - `put_literal(...)` values,
  - `start_fn(...)` `argv` values,
  - `start_fn(...)` `kwargv` values.
- Function execution:
  - first call arg resolves to runnable,
  - kwargs resolve inner-most to outer-most by key,
  - unknown key raises `DmlRepoError("Unknown kwarg: <key>")`.
  - non-builtin execution always performs remote cache lookup by argv identity before adapter invocation.
  - adapter `succeeded` status requires cache-backed result resolution; missing cache entry is a deterministic failure.
- `put_import(...)` stages `ImportNode` refs from committed DAG nodes.
- `commit(...)` writes new commit and deletes index ref.
- If `head=` is provided, `commit(...)` updates that head; if omitted, commit is detached and returned.
- Invariants:
  - index methods validate index ownership and DAG membership,
  - function result DAG contains `result` or `error`,
  - commit finalization writes commit state and removes index ref.

## References

- [../../codec-system.md](../../codec-system.md)
- [../../adapter-execution-contract.md](../../adapter-execution-contract.md)
- [../../execution-model.md](../../execution-model.md)
- [../../remote-data-model.md](../../remote-data-model.md)
