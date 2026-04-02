# Contrib Module (`daggerml.contrib`)

## Status

specified

## Authority

This document is authoritative for `daggerml.contrib` module boundaries and responsibilities.
If contrib docs conflict on module ownership, this document is the source of truth.


## Purpose

Contrib architecture defines module boundaries and dependency directions within `daggerml.contrib`.


## Scope

This document defines:

- contrib package boundaries,
- module-level responsibilities,
- allowed dependency directions between contrib modules.

This document does not redefine core execution/storage contracts.


## Contract References

Core contracts used by contrib are defined in:

- [../adapter-execution-contract.md](../adapter-execution-contract.md)
- [../execution-model.md](../execution-model.md)
- [../internal/storage.md](../internal/storage.md)

Contrib docs navigation and recommended reading order are defined in:

- [overview.md](overview.md)


## Content

## Planning Status

Contrib docs in this directory define a target architecture plan.
They are normative for contrib design intent, not a claim that contrib modules are already implemented in this repository.

## Module Inventory

`daggerml.contrib` MUST contain these module areas:

- `daggerml.contrib.api`: `@dagclass`, `@funkify`, delayed-action helpers, and dagclass run surface.
- `daggerml.contrib.adapter_registry`: normalized adapter registration and lookup contracts.
- `daggerml.contrib.executor_registry`: normalized executor registration and lookup contracts.
- `daggerml.contrib.executor_state`: executor kickoff/poll state abstractions.
- `daggerml.contrib.codecs`: contrib-owned literal codecs.
- `daggerml.contrib.funks`: contrib-owned prebuilt funks.
- `daggerml.contrib.testing`: contrib-owned testing helpers.
- `daggerml.contrib.status`: contrib runtime status/introspection API.
- `daggerml.contrib.adapters`: adapter framework and built-in adapter definitions.
- `daggerml.contrib.s3`: `S3Store` utility surface.


## Canonical Contracts

Contrib runtime architecture contracts are centralized in:

- [runtime-contract.md](runtime-contract.md)

This canonical runtime contract owns:

- adapter/executor split,
- kickoff/poll lifecycle and state backend requirements,
- runtime-harness supervisor role and contract boundaries,
- contrib runtime error categories.

The following docs are focused companion references that MUST remain consistent with the canonical runtime contract:

- [execution-graph.md](execution-graph.md)
- [registries.md](registries.md)
- [executor-state.md](executor-state.md)
- [executor-catalog.md](executor-catalog.md)


## Module Responsibilities

`daggerml.contrib.api`:

- define `@dagclass` and `@funkify` API shape,
- build `DelayedRunnable`/`Runnable` chains from callables/scripts/runnables,
- normalize callable vs sub inputs for adapter runnable resolution,
- consume adapter registry lookups used by `funkify` dispatch,
- defer adapter/executor-specific runnable resolution details to [runtime-contract.md](runtime-contract.md) and per-executor kwargs/schema details to [executor-catalog.md](executor-catalog.md).

Contrib registries:

- define adapter/executor registry reference contracts,
- define `AdapterSpec` and `ExecutorSpec` reference shapes used at runtime,
- provide plugin packaging/discovery reference examples.

Contrib executor-state:

- define shared kickoff/poll state contracts for executors,
- define backend contracts for `LocalState` and `DynamoState`,
- define shared state record ownership and metadata conventions used by wrappers and deepest executors,
- defer live execution-graph schema and cancel/sweep ownership to [execution-graph.md](execution-graph.md).

`daggerml.contrib.status`:

- define contrib runtime status API for adapters/executors/codecs visibility,
- provide the structured diagnostics payload used by contrib CLI pass-through surfaces under `dml contrib`.

`daggerml.contrib.codecs`:

- define contrib-owned literal codec availability and serialization behavior,
- define dataframe-specific externalization behavior for optional dataframe backends,
- defer generic codec loading/order semantics to [../codec-system.md](../codec-system.md),
- defer S3 utility behavior to [s3-store.md](s3-store.md).

`daggerml.contrib.funks`:

- define contrib-owned prebuilt funk surfaces,
- define contrib-owned invocation/result contracts for those funks,
- defer generic `api.funkify` behavior to [api.md](api.md),
- defer runtime execution behavior to [runtime-contract.md](runtime-contract.md).

`daggerml.contrib.testing`:

- define contrib-owned testing helpers for author-code unit tests,
- define node-like testing helpers that intentionally expose only the contrib-facing `.value()` protocol,
- defer real DAG/runtime semantics to [api.md](api.md) and [runtime-contract.md](runtime-contract.md).

`daggerml.contrib.adapters`:

- define adapter framework contracts and `BaseAdapter` registration behavior,
- provide adapter entry points and adapter-executable registrations named by `Runnable.adapter`,
- define adapter CLI dispatch (`Adapter.cli(...)`) and parsed one-step send dispatch (`Adapter.send(...)`) behavior,
- provide the contrib adapter-side ingress/egress portion of the core adapter contract,
- orchestrate adapter dispatch together with contrib executors so the selected adapter/executor pair satisfies the execution-surface runtime contract.

`daggerml.contrib.s3`:

- provide `S3Store` APIs for URI parsing, object operations, JSON helpers, and archive helpers,
- provide reproducible directory-to-CAS upload behavior for local source trees,
- serve shared S3 utility behavior for user code and contrib adapters/executors.


## Dependency Direction

Allowed direct dependencies:

- `daggerml.contrib.api` -> core execution contracts.
- `daggerml.contrib` registries -> core error/runtime contracts only.
- `daggerml.contrib.executor_state` -> core error/runtime contracts and backend client contracts only.
- `daggerml.contrib.adapters` -> core adapter/execution contracts, adapter/executor registries, executor state, optionally `daggerml.contrib.s3`.
- `daggerml.contrib.s3` -> core storage/remote utility dependencies.
- `daggerml.contrib.codecs` -> core codec contracts and optionally `daggerml.contrib.s3`.
- `daggerml.contrib.funks` -> contrib API contracts and optionally `daggerml.contrib.s3`.
- `daggerml.contrib.testing` -> contrib API contracts only.

Disallowed direct dependency:

- `daggerml.contrib.s3` MUST NOT depend on `daggerml.contrib.adapters`.


## Per-Module Detail Docs

- canonical contrib runtime contract: [runtime-contract.md](runtime-contract.md)
- `daggerml.contrib.api` behavior is defined in [api.md](api.md).
- Contrib codec behavior is defined in [codecs.md](codecs.md).
- Contrib prebuilt funk behavior is defined in [funks.md](funks.md).
- Contrib testing-helper behavior is defined in [testing.md](testing.md).
- Contrib adapter/executor registry behavior is defined in [registries.md](registries.md).
- Contrib executor-state behavior is defined in [executor-state.md](executor-state.md).
- Contrib per-executor runtime behavior is defined in [executor-catalog.md](executor-catalog.md).
- Contrib status/introspection behavior is defined in [status.md](status.md).
- `S3Store` behavior is defined in [s3-store.md](s3-store.md).


## References

- [runtime-contract.md](runtime-contract.md)
- [registries.md](registries.md)
- [codecs.md](codecs.md)
- [funks.md](funks.md)
- [testing.md](testing.md)
- [executor-state.md](executor-state.md)
- [executor-catalog.md](executor-catalog.md)
- [status.md](status.md)
- [api.md](api.md)
