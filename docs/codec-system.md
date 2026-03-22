---
status: specified
doc_type: spec
---

# Codec System

## Authority

This document is authoritative for codec-system contracts in `daggerml._internal.codec`, including codec interface, codec application semantics, and plugin loading.

## Scope

This doc defines codec interface and lifecycle contracts, plugin loading contracts, ordering rules, and error behavior.

## Purpose

Define the canonical codec-system contract used for normalization on write/execution staging paths.

## Glossary

- Codec: Component that serializes objects for write/execution staging.
- `DmlRepoError`: Raised on plugin load/registration failure.
- `Runnable`: Executable value.
- Plugin discovery group: `daggerml.codecs`.

## Contract

### Interfaces

- `apply_codec(value, ctx=...)`: Runtime owners call this to normalize inputs.
- `can_encode(obj) -> bool`: Interface to determine if a codec can encode an object.
- `encode(obj, ctx) -> Any`: Interface to encode an object.
- `ctx`: Context object that includes `index_ref` and `index_ops`.
- Plugin return shapes allowed:
  - codec object,
  - `(codec, priority)`,
  - sequence of either.
- Plugin discovery group is `daggerml.codecs`.

### Invariants

- Codec application is write-path normalization only.
- Codec evaluation MUST happen outside active write transactions for the normalized write/staging path.
- Codec side effects SHOULD be idempotent/repeatable.
- Selection order is deterministic: priority (higher first), then registration order.
- Evaluation short-circuits on first matching codec.
- Re-encoding behavior: if encoded output differs, codec selection is re-applied to the new value until convergence/failure.
- If `encode(...)` returns a collection value or a `Runnable`, traversal MUST continue recursively over that returned value.
- Plugin loading is lazy and deterministic.
- Traversal/staging/write orchestration is owned by runtime owners, not codec implementations.

### Error Semantics

- Plugin load/registration failures MUST raise deterministic `DmlRepoError` values.

### Authority Handoffs

- `DmlRepoError` contracts are defined in `errors.md`.
- `index_ops` contracts are defined in `internal/ops/index-ops.md`.

## Compatibility

- Codec-system changes MUST NOT expand public API except the plugin discovery mechanism (`daggerml.codecs` entry-point group).

## References

- [internal/ops/index-ops.md](internal/ops/index-ops.md)
- [errors.md](errors.md)
- [src/daggerml/_internal/codec.py](../src/daggerml/_internal/codec.py)
