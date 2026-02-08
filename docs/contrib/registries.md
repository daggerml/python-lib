---
status: specified
doc_type: spec
---

# Registries

## Authority

This document is authoritative for contrib adapter/executor registry reference shapes and plugin packaging/discovery contracts.

## Purpose

Define object-shape contracts used by contrib registries and plugin entry-point examples.

## Scope

This doc defines `AdapterSpec` and `ExecutorSpec` reference shapes and registry plugin discovery groups.

## Content

- Registry definitions are object-based (class or instance) and consumed through attribute access.
- `AdapterSpec` registry/discovery attributes:
  - `name`
  - `executable`
- `ExecutorSpec` registry/discovery attributes:
  - `adapter`
- Runtime adapter/executor class surface requirements are authoritative in [runtime-contract.md](runtime-contract.md); this registry document is informative-only for runtime class-method contracts.
- Adapter plugin entry-point group:
  - `daggerml.contrib.adapters`
- Executor plugin entry-point group:
  - `daggerml.contrib.executors`
- Entry-point target return contract:
  - one object definition,
  - iterable of object definitions,
  - callable returning either.
- Example adapter plugin object:
  - class attributes or instance attributes are both valid as long as registry/discovery attributes are readable.
- Example executor plugin object:
  - must expose `adapter`; runtime contract defines runtime-required class surfaces.
- Executor-specific runnable kwargs/schema contracts are out of scope for this registry reference and are authoritative in [executor-catalog.md](executor-catalog.md).

## References

- [runtime-contract.md](runtime-contract.md)
- [api.md](api.md)
- [executor-catalog.md](executor-catalog.md)
