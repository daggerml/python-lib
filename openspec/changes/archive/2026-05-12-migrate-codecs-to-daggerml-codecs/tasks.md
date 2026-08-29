## 1. Stage 1 Extraction

- [x] 1.1 Create `src/daggerml/codecs.py` and move codec registry, plugin loading, `CodecContext`, and built-in codec implementations into it.
- [x] 1.2 Move delayed-action codec types and behavior into `daggerml.codecs`, and update `daggerml.api`, `daggerml.contrib.api`, and `_internal` imports to consume codec symbols from that module.
- [x] 1.3 Introduce a codec-local error type in `daggerml.codecs` and translate codec failures back to repository-domain errors at `_internal` call sites.
- [x] 1.4 Update codec-related tests to confirm Stage 1 preserves current behavior while removing codec logic from `daggerml._internal.*`.

## 2. Stage 2 Contract Migration

- [x] 2.1 Change the codec contract so built-in codecs and plugin codecs receive `daggerml.api.Dag` instead of `CodecContext`.
- [x] 2.2 Implement a `Dag`-owned recursive codec normalization and insertion helper, and use it from `Dag.put`.
- [x] 2.3 Update `Dag.call` to insert the callable, positional arguments, and keyword argument values through the codec-driven normalization path before runtime execution.
- [x] 2.4 Remove codec traversal and `CodecContext` usage from `_internal` runtime staging paths once `Dag` owns normalization.

## 3. Validation And Cleanup

- [x] 3.1 Update codec and API documentation to reflect the unified codec module and the Stage 2 `Dag` contract.
- [x] 3.2 Verify plugin discovery still works through the `daggerml.codecs` entry-point group after both stages.
- [x] 3.3 Run the relevant codec, API, and contrib test coverage for both stages and fix any regressions.
