# Contrib Module (`daggerml.contrib`)

## Status

specified

## Authority

This document is authoritative for `daggerml.contrib` module boundaries and responsibilities.

## Purpose

Define contrib package boundaries and dependency directions within `daggerml.contrib`.

## Scope

This document defines contrib package boundaries, module-level responsibilities, and allowed dependency directions. It does not redefine core execution or storage contracts.

## Canonical Contracts

Contrib runtime architecture contracts are centralized in [runtime-contract.md](runtime-contract.md).

Focused companion references that must remain consistent with it:

- [registries.md](registries.md)
- [executor-state.md](executor-state.md)
- [executor-catalog.md](executor-catalog.md)
- [status.md](status.md)

## Module Inventory

`daggerml.contrib` contains these module areas:

- `daggerml.contrib.api`
- `daggerml.contrib.adapter_registry`
- `daggerml.contrib.executor_registry`
- `daggerml.contrib.executor_state`
- `daggerml.contrib.codecs`
- `daggerml.contrib.funks`
- `daggerml.contrib.testing`
- `daggerml.contrib.status`
- `daggerml.contrib.adapters`
- `daggerml.contrib.s3`

## Module Responsibilities

- `daggerml.contrib.api`: build delayed runnable chains and defer runtime specifics to contrib runtime contracts.
- registries: define normalized adapter and executor registration and lookup.
- `daggerml.contrib.executor_state`: define the shared `ExecutionState` contract and `ExecutionRecord` shape used by built-in runtimes.
- `daggerml.contrib.status`: define contrib runtime status and diagnostics payloads.
- `daggerml.contrib.adapters`: define adapter framework contracts, adapter CLI dispatch, and adapter-side ingress and egress behavior.
- `daggerml.contrib.s3`: provide shared S3 utility behavior for user code and contrib runtimes.

## Dependency Direction

Allowed direct dependencies:

- `daggerml.contrib.api` -> core execution contracts
- registries -> core error and runtime contracts only
- `daggerml.contrib.executor_state` -> core error and backend client contracts only
- `daggerml.contrib.adapters` -> core execution contracts, registries, executor state, optionally `daggerml.contrib.s3`
- `daggerml.contrib.s3` -> core storage and remote utility dependencies
- `daggerml.contrib.codecs` -> core codec contracts and optionally `daggerml.contrib.s3`
- `daggerml.contrib.funks` -> contrib API contracts and optionally `daggerml.contrib.s3`
- `daggerml.contrib.testing` -> contrib API contracts only

Disallowed:

- `daggerml.contrib.s3` MUST NOT depend on `daggerml.contrib.adapters`.

## References

- [runtime-contract.md](runtime-contract.md)
- [registries.md](registries.md)
- [executor-state.md](executor-state.md)
- [executor-catalog.md](executor-catalog.md)
- [status.md](status.md)
- [api.md](api.md)
