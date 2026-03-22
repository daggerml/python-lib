---
status: specified
doc_type: spec
---

# Registries

## Authority

This document is authoritative for contrib adapter/executor registry reference shapes and plugin packaging/discovery contracts.

## Scope

This doc defines `AdapterSpec` and `ExecutorSpec` reference shapes and registry plugin discovery groups. Executor-specific runnable kwargs/schema contracts are out of scope for this registry reference.

## Purpose

Define object-shape contracts used by contrib registries and plugin entry-point examples.

## Glossary

- AdapterSpec: A registry reference shape for adapters, defined normatively in this document.
- ExecutorSpec: A registry reference shape for executors, defined normatively in this document.

## Contract

### Interfaces

**AdapterSpec Registry Attributes**
- Shape: Object-based (class or instance).
- Required attributes: `name`, `executable`.
- Behavior: Attributes must be readable via attribute access.
- Unspecified fields are ignored.

**ExecutorSpec Registry Attributes**
- Shape: Object-based (class or instance).
- Required attributes: `adapter`.
- Behavior: Attributes must be readable via attribute access.
- Unspecified fields are ignored.

**Plugin Discovery Groups**
- Adapter plugin entry-point group: `daggerml.contrib.adapters`
- Executor plugin entry-point group: `daggerml.contrib.executors`
- Entry-point target return contract: MUST return one object definition, an iterable of object definitions, or a callable returning either.
- Unspecified fields are ignored.

### Invariants

- Registry definitions MUST be object-based (class or instance) and consumed through attribute access.
- Adapter plugin objects MUST expose registry/discovery attributes that are readable.
- Executor plugin objects MUST expose `adapter`.

### Error Semantics

- Invalid entry-point targets or missing required attributes (`name`, `executable`, `adapter`) result in discovery failure.
- This is a terminal, non-retryable error. The caller MUST provide a compliant object definition.

### Authority Handoffs

- Runtime adapter/executor class surface requirements are authoritative in [runtime-contract.md](runtime-contract.md); this registry document is informative-only for runtime class-method contracts.
- Executor-specific runnable kwargs/schema contracts are authoritative in [executor-catalog.md](executor-catalog.md).

## Compatibility

- The registry attributes (`name`, `executable`, `adapter`) and entry-point groups form a stable contract. Backward compatibility will be maintained for discovery attribute names and return shapes.

## References

- [runtime-contract.md](runtime-contract.md)
- [api.md](api.md)
- [executor-catalog.md](executor-catalog.md)
