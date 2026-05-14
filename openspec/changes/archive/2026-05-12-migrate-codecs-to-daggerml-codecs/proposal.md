## Why

Codec behavior is currently split across `daggerml._internal`, `daggerml.api`, and `daggerml.contrib.api`, which makes the ownership boundary unclear and couples internal staging code to public wrapper concerns. We want a staged migration that first centralizes codec code in one public module, then moves the codec contract and traversal ownership to `Dag` without changing plugin extensibility.

## What Changes

- Add a single codec module at `src/daggerml/codecs.py` that owns codec registration, plugin loading, codec types, and built-in codec implementations.
- Move all existing codec logic out of `daggerml._internal.*`, `daggerml.api`, and `daggerml.contrib.api` into `daggerml.codecs`.
- Stage 1: preserve the current runtime contract by continuing to call codecs from `_internal` with `CodecContext`, while translating codec-local errors back to repository-domain errors at the `_internal` boundary.
- Stage 2: change the codec contract so codecs receive `daggerml.api:Dag`, move recursive codec traversal and insertion ownership into `Dag` methods, and remove `CodecContext` entirely.
- Keep `Node` as a built-in codec, keep plugin discovery under the `daggerml.codecs` entry-point group, and update `Dag.call` to insert callable and argument values before invoking runtime execution.

## Capabilities

### New Capabilities
- `codec-normalization`: Defines the codec module boundary, built-in codec behavior, plugin contract, and staged migration of codec traversal from `_internal` to `daggerml.api.Dag`.

### Modified Capabilities

## Impact

- Affected code: `src/daggerml/codecs.py`, `src/daggerml/api.py`, `src/daggerml/contrib/api.py`, `src/daggerml/_internal/__init__.py`, `src/daggerml/_internal/ops/index.py`, and codec-related tests.
- Affected APIs: codec plugin `encode(...)` contract, internal codec error translation, and `Dag`-owned staging/insert behavior.
- Affected packaging: the `daggerml.codecs` plugin entry-point group remains in place but now targets the unified codec module.
