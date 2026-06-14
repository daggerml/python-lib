## Why

`dml.runtime.describe_graph()` currently returns only a raw graph payload. That is fine for programmatic callers, but awkward for humans who want a quick runtime lineage view from Python or the CLI.

## What Changes

- Extend `Dml.runtime.describe_graph` with a `visual: bool = False` option.
- Keep the current raw `ExecutionGraph` payload when `visual` is `False`.
- Render a human-friendly execution graph view and return `None` when `visual` is `True`.
- Add an optional `rich` dependency used only for visual graph rendering.
- Keep graph extraction in execution-state code and keep presentation outside `exec_state.py`.

## Capabilities

### New Capabilities
<!-- None. -->

### Modified Capabilities
- `unified-dml-surface`: `describe_graph` gains an optional visual rendering mode with different return behavior.

## Impact

- Affected code: `src/daggerml/_core/dml.py` and a small rendering helper location if needed.
- Affected packaging: `pyproject.toml` gains an optional dependency for `rich`.
- Affected tests: shared DML surface tests and runtime/CLI behavior tests for raw vs visual behavior.
- API impact: `describe_graph(..., visual=True)` becomes a presentation-oriented path that returns `None` instead of an `ExecutionGraph` payload.
