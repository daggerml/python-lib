## Context

Codec logic is currently split across three places: registry and traversal in `daggerml._internal.codec`, `NodeCodec` in `daggerml.api`, and delayed-action codec behavior in `daggerml.contrib.api`. This makes `_internal` responsible for behavior that depends on public wrapper types and contrib-facing delayed values.

The change is intentionally staged. Stage 1 is a relocation only: `src/daggerml/codecs.py` becomes the single home for codec code, while `_internal` continues to call codecs exactly as it does today through `CodecContext`. Stage 2 then changes ownership and contract: codecs receive `Dag`, recursive traversal moves to `daggerml.api.Dag`, and `_internal` stops normalizing values.

## Goals / Non-Goals

**Goals:**
- Establish `src/daggerml/codecs.py` as the only module that contains codec logic and codec types.
- Preserve current behavior during Stage 1, including plugin loading, `Node` handling, delayed-action handling, and `_internal` call sites.
- In Stage 2, move recursive normalization and insertion ownership to `daggerml.api.Dag`.
- Preserve the `daggerml.codecs` plugin entry-point group across both stages.

**Non-Goals:**
- Redesign codec matching, priority, or convergence semantics.
- Remove `Node` as a codec.
- Introduce a second plugin system or new codec registration surface.
- Change adapter execution, DAG storage format, or non-codec staging semantics.

## Decisions

### Create one codec module at `src/daggerml/codecs.py`
All codec code moves into `daggerml.codecs`: registry, plugin loading, codec protocol, built-in codecs, delayed-action value types, and traversal helpers. This gives both stages a single implementation target and removes split ownership across public, contrib, and internal modules.

Alternative considered: keep built-in codecs in `api.py` and `contrib/api.py` while only moving the registry. Rejected because it would preserve the same ownership split that this change is trying to remove.

### Use a codec-local error type in Stage 1
Stage 1 avoids importing `_internal.types` from `daggerml.codecs`. Codec failures therefore raise a codec-local exception type, and `_internal` translates that exception back into repository-domain errors at its boundary. This keeps Stage 1 as a pure extraction while preserving outward behavior.

Alternative considered: keep `daggerml.codecs` dependent on `_internal.types.DmlRepoError`. Rejected because it would leave the new module coupled to `_internal`, making Stage 2 harder.

### Keep `CodecContext` only for Stage 1
Stage 1 keeps the existing contract so `_internal` call sites do not change behavior. Stage 2 removes `CodecContext` entirely and passes `Dag` into codecs. This separates extraction from behavior change and reduces migration risk.

Alternative considered: switch directly to `Dag` during extraction. Rejected because it would mix module relocation with behavioral changes in call-site ownership.

### Make `Dag` own recursive normalization in Stage 2
In Stage 2, one `Dag`-owned helper recursively walks values, applies codecs, preserves or imports nodes, and prepares values for runtime staging. `Dag.put` and `Dag.call` use this helper before delegating to runtime methods. `Dag.call` inserts the callable and all arguments before continuing with execution.

Alternative considered: keep recursion in `_internal` and only change the codec argument from `CodecContext` to `Dag`. Rejected because traversal ownership is the core layering problem.

## Risks / Trade-offs

- [Import-cycle risk during Stage 1] -> Move delayed-action types and built-in codecs into `daggerml.codecs` together rather than splitting them across modules.
- [Behavior drift between Stage 1 and Stage 2] -> Treat Stage 1 as a no-semantics-change extraction and cover current codec behavior with tests before changing ownership.
- [Error-surface mismatch after introducing codec-local errors] -> Translate codec-local failures to repository-domain failures at `_internal` call sites until Stage 2 removes those boundaries.
- [Plugin breakage when the encode contract changes to `Dag`] -> Keep the entry-point group stable, document the new argument contract clearly, and migrate built-in codecs first.

## Migration Plan

1. Stage 1: create `src/daggerml/codecs.py`, move all codec logic there, and update `_internal` to import codec symbols from that module while preserving `CodecContext` call sites.
2. Stage 1: introduce codec-local errors and translate them to repository-domain errors at `_internal` boundaries.
3. Stage 2: move recursive normalization into `daggerml.api.Dag` and change codec `encode(...)` to receive `Dag`.
4. Stage 2: remove `_internal` codec traversal, remove `CodecContext`, and update plugin and built-in codec implementations to the `Dag` contract.

## Open Questions

- Whether delayed-action helper types continue to be re-exported from `daggerml.contrib.api` after moving their implementation into `daggerml.codecs`.
- Whether Stage 2 should keep a separate public helper for codec-driven insertion or keep it fully internal to `Dag` methods.
