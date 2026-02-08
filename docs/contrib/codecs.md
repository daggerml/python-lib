---
status: specified
doc_type: spec
---

# Contrib Codecs

## Authority

This document is authoritative for contrib-owned literal codec behavior in `daggerml.contrib.codecs`.

This document owns:

- the set of contrib codecs currently provided by `daggerml.contrib.codecs`,
- availability rules for those codecs within a process,
- serialization behavior for those codecs.

This document does not own core codec plugin discovery, codec ordering semantics, or generic codec interface rules.

## Scope

In scope:

- contrib codecs implemented in `src/daggerml/contrib/codecs.py`,
- optional dependency gating for those codecs,
- dataframe serialization outputs produced by those codecs.

Out of scope:

- generic codec discovery/loading behavior,
- non-contrib codecs,
- `S3Store` API semantics,
- external `Uri` lifecycle semantics beyond the returned value shape.

## Purpose

Define the contrib-specific codec catalog and the serialization contract for the currently implemented dataframe codecs.

## Glossary

- Contrib Codec: a literal codec defined by `daggerml.contrib.codecs`.
- Pandas DataFrame Codec: the Contrib Codec that matches `pandas.DataFrame` values.
- Polars DataFrame Codec: the Contrib Codec that matches `polars.DataFrame` values.
- Dataframe Artifact: the parquet payload bytes produced by a Contrib Codec for a dataframe value.

## Contract

### Interfaces

- Location:
  - `daggerml.contrib.codecs`
- Required interface:
  - `literal_codecs() -> list[object]`
- `literal_codecs()` MUST return only Contrib Codecs whose optional backend dependency is installed in the current Python process.
- `literal_codecs()` MUST return codecs in this order when available:
  - Pandas DataFrame Codec,
  - Polars DataFrame Codec.
- Current Contrib Codec catalog:
  - Pandas DataFrame Codec for `pandas.DataFrame`,
  - Polars DataFrame Codec for `polars.DataFrame`.
- Pandas DataFrame Codec behavior:
  - `can_encode(value)` MUST return true only for `pandas.DataFrame` instances,
  - `encode(value, ctx)` MUST serialize the dataframe as parquet bytes,
  - parquet serialization MAY require an additional parquet engine provided by the pandas runtime environment,
  - `encode(value, ctx)` MUST store those bytes through `S3Store.put(..., suffix=".parquet")`,
  - `encode(value, ctx)` MUST return the resulting `Uri`.
- Polars DataFrame Codec behavior:
  - `can_encode(value)` MUST return true only for `polars.DataFrame` instances,
  - `encode(value, ctx)` MUST serialize the dataframe as parquet bytes,
  - `encode(value, ctx)` MUST store those bytes through `S3Store.put(..., suffix=".parquet")`,
  - `encode(value, ctx)` MUST return the resulting `Uri`.
- When a backend dependency is not installed, the corresponding Contrib Codec MUST be absent from `literal_codecs()` output.
- `ctx` is accepted for codec interface compatibility and MUST NOT change dataframe artifact format or storage addressing behavior.

### Invariants

- Contrib Codecs MUST serialize dataframe payload bytes outside repository storage and represent the result as a `Uri`.
- Dataframe Artifact addressing MUST be content-addressed through `S3Store.put(...)`.
- Dataframe Artifact URIs returned by Contrib Codecs MUST identify parquet objects.
- `literal_codecs()` output MUST be deterministic for a fixed set of installed optional backend dependencies.

### Error Semantics

- Missing optional backend dependency:
  - non-retryable until the dependency is installed,
  - non-terminal for `literal_codecs()` overall,
  - caller behavior: treat the corresponding codec as unavailable,
  - operator action: install the missing backend package if that codec is required.
- Missing pandas parquet engine:
  - non-retryable until a supported parquet engine is installed,
  - terminal for that `encode(...)` call,
  - caller behavior: surface the pandas serialization exception,
  - operator action: install a pandas-supported parquet engine.
- Dataframe serialization failure:
  - retryability unspecified,
  - terminal for that `encode(...)` call,
  - caller behavior: surface the exception from the dataframe backend or storage layer,
  - operator action: correct the dataframe value, backend installation, or storage configuration.
- Artifact write failure through `S3Store`:
  - retryability is determined by the storage failure,
  - terminal for that `encode(...)` call,
  - caller behavior: surface the storage exception,
  - operator action: restore valid S3 configuration or availability.

### Observability

- When Contrib Codecs are loaded into the runtime codec registry, contrib status reporting MUST identify them through the `codecs` registration list defined by `docs/contrib/status.md`.
- The presence or absence of Pandas DataFrame Codec and Polars DataFrame Codec in runtime status MUST reflect the installed optional backend dependencies and runtime codec loading state.

### Authority Handoffs

- Generic codec interface, plugin loading, and codec ordering are authoritative in [../codec-system.md](../codec-system.md).
- `S3Store` API and content-addressed write semantics are authoritative in [s3-store.md](s3-store.md).
- External `Uri` storage semantics are authoritative in [../storing-and-retrieving-external-data.md](../storing-and-retrieving-external-data.md).
- Contrib status/introspection output is authoritative in [status.md](status.md).

## Compatibility

- This document defines only the currently implemented Contrib Codec catalog.
- Adding a new Contrib Codec requires updating this document.
- Changing dataframe artifact format away from parquet or changing the returned value away from `Uri` is a compatibility-relevant contract change.
- Reordering the codecs returned by `literal_codecs()` is compatibility-relevant because runtime codec evaluation order is externally observable.

## References

- [../codec-system.md](../codec-system.md)
- [s3-store.md](s3-store.md)
- [../storing-and-retrieving-external-data.md](../storing-and-retrieving-external-data.md)
- [status.md](status.md)
