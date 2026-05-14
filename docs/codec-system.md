---
status: specified
doc_type: spec
---

# Codec System

## Authority

This document is authoritative for codec-system contracts in `daggerml.codecs`, including codec interface, codec application semantics, built-in codec behavior, and plugin loading.

## Scope

This doc defines codec interface and lifecycle contracts, plugin loading contracts, ordering rules, and error behavior.

## Purpose

Define the canonical codec-system contract used for normalization on public DAG staging and call-entry paths.

## Glossary

- Codec: Component that serializes objects for write/execution staging.
- `CodecError`: Internal codec failure raised by codec loading or codec evaluation.
- `Runnable`: Executable value.
- Plugin discovery group: `daggerml.codecs`.

## Contract

### Interfaces

- `apply_codec(value, ctx=...)`: Applies codec matching and re-encoding to one value.
- `normalize_codec_value(value, ctx=...)`: Applies codecs and recursively traverses returned collections and runnable fields.
- `stage_value(dag, value, name=None)`: Dag-owned staging helper that normalizes a value and inserts it into the active DAG.
- `can_encode(obj) -> bool`: Interface to determine if a codec can encode an object.
- `encode(obj, ctx) -> Any`: Interface to encode an object.
- `ctx`: Active `daggerml.api.Dag` instance.
- Plugin return shapes allowed:
  - codec object,
  - `(codec, priority)`,
  - sequence of either.
- Plugin discovery group is `daggerml.codecs`.
- Built-in codecs are `NodeCodec` and `DelayedActionCodec` in `daggerml.codecs`.

### Invariants

- Codec application is API-owned write and call-entry normalization only.
- Codec evaluation MUST happen before runtime staging begins.
- Codec side effects SHOULD be idempotent/repeatable.
- Selection order is deterministic: priority (higher first), then registration order.
- Evaluation short-circuits on first matching codec.
- Re-encoding behavior: if encoded output differs, codec selection is re-applied to the new value until convergence/failure.
- If `encode(...)` returns a collection value or a `Runnable`, traversal MUST continue recursively over that returned value.
- Plugin loading is lazy and deterministic.
- Traversal and staging orchestration are owned by `daggerml.codecs.stage_value(...)` and `daggerml.api.Dag` call sites, not by codec implementations.

### Error Semantics

- Plugin load/registration failures MUST raise deterministic codec failures.
- Public DAG staging and call-entry surfaces MUST translate codec failures into the repository-domain error surface they already expose.

### Authority Handoffs

- Public DAG staging and call-entry contracts are defined in `api.md`.
- Repository-domain error contracts are defined in `errors.md`.

## Compatibility

- Codec-system changes MUST NOT expand public API except the plugin discovery mechanism (`daggerml.codecs` entry-point group).

## References

- [api.md](api.md)
- [errors.md](errors.md)
- [src/daggerml/codecs.py](../src/daggerml/codecs.py)
