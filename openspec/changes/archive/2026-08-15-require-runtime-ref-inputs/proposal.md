## Why

`Dml.runtime` currently mixes exact runtime identity across `Ref` objects and execution-id strings, including coercing ref-shaped strings inside `Dml`. This makes the Python contract ambiguous and lets CLI transport concerns leak into the core API.

## What Changes

- **BREAKING** Require `Ref` for every `Dml.runtime` input that identifies a runtime or execution, including execution-aware `create`, execution-record and launch-state reads, graph roots, and cancellation targets.
- Remove all string-to-`Ref` coercion and `Ref | str` runtime identity parameters from `Dml`.
- Keep lower-level execution-state persistence and coordination keyed by string execution IDs; `Dml.runtime` extracts those IDs from supplied refs before delegation.
- Keep the generated CLI responsible for converting textual `<namespace>:<id>` arguments into `Ref` objects before invoking `Dml`.
- Update contrib callers, public wrappers, tests, and runtime documentation to construct or pass refs at the `Dml` boundary.
- Leave revision expressions and revision resolution unchanged; revisions remain string selectors and are outside this change.

## Capabilities

### New Capabilities

None.

### Modified Capabilities

- `unified-dml-surface`: make runtime and execution identity inputs consistently `Ref`-only across the shared `Dml.runtime` surface while retaining string IDs below that boundary.

## Impact

- Affected code: `src/daggerml/_core/dml.py`, generated CLI transport tests, `daggerml.api` and contrib runtime callers, especially the supervisor.
- Affected API: direct Python callers of `Dml.runtime.create`, `read_launch_state`, `read_execution_record`, `describe_graph`, and `cancel` must pass `Ref` values instead of execution-id or ref-shaped strings.
- Affected contracts and docs: the unified DML surface spec, runtime contract tests, CLI examples, runtime concepts/reference material, and architecture documentation.
- Lower-level `IndexOps`, `ExecutionState`, adapter envelopes, persisted execution records, and revision selector APIs remain string-ID based and do not change storage formats.
