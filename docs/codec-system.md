---
status: specified
doc_type: spec
---

# Codec System

## Authority

This document is authoritative for codec-system contracts in `daggerml._internal.codec`, including codec interface, codec application semantics, and plugin loading.

## Purpose

Define the canonical codec-system contract used for normalization on write/execution staging paths.

## Scope

This doc defines codec interface and lifecycle contracts, plugin loading contracts, ordering rules, and error behavior.

## Content

- Runtime owners call `apply_codec(value, ctx=...)` to normalize inputs.
- Codec application is write-path normalization only.
- Codec interface:
  - `can_encode(obj) -> bool`
  - `encode(obj, ctx) -> Any`
- `ctx` includes `index_ref` and `index_ops`.
- Codec evaluation MUST happen outside active write transactions for the normalized write/staging path.
- Codec side effects SHOULD be idempotent/repeatable.
- Selection order is deterministic: priority (higher first), then registration order.
- Evaluation short-circuits on first matching codec.
- Re-encoding behavior: if encoded output differs, codec selection is re-applied to the new value until convergence/failure.
- If `encode(...)` returns a collection value or a `Runnable`, traversal MUST continue recursively over that returned value.
- Plugin discovery group is `daggerml.codecs`.
- Plugin loading is lazy and deterministic.
- Plugin load/registration failures raise deterministic `DmlRepoError` values.
- Plugin return shapes allowed:
  - codec object,
  - `(codec, priority)`,
  - sequence of either.
- Traversal/staging/write orchestration is owned by runtime owners, not codec implementations.
- Codec-system changes MUST NOT expand public API except the plugin discovery mechanism (`daggerml.codecs` entry-point group).

## References

- [internal/ops/index-ops.md](internal/ops/index-ops.md)
- [errors.md](errors.md)
- [src/daggerml/_internal/codec.py](../src/daggerml/_internal/codec.py)
