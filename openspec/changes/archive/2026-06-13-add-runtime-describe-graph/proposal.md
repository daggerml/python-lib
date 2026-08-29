## Why

Operators currently have no runtime-owned way to inspect the execution lineage rooted at the currently open indexes. The execution state already records lifecycle and active child information, but it does not expose a descendant graph payload that callers can retrieve directly from `ExecutionState`.

## What Changes

- Add a public runtime inspection method named `Dml.runtime.describe_graph(*roots: Ref | str)`.
- Default `describe_graph()` with no explicit roots to all currently open local runtime indexes.
- Extend execution records with durable lineage metadata so completed descendants remain visible after they leave the active spawned set.
- Add execution-state-owned graph extraction that returns descendant execution metadata using only execution record objects.
- Rename the existing spawned-child completion helper so it moves terminal descendants from `spawned_execution_ids` into `child_execution_ids` instead of only dropping them.

## Capabilities

### New Capabilities
<!-- None. -->

### Modified Capabilities
- `runtime-execution-records`: execution records gain durable child lineage metadata, creation timestamps, and a record-only descendant graph query surface.
- `unified-dml-surface`: the public runtime namespace gains `describe_graph` for execution-graph inspection.

## Impact

- Affected code: `src/daggerml/_core/exec_state.py`, `src/daggerml/_core/index.py`, and `src/daggerml/_core/dml.py`.
- Affected tests: execution coordination/runtime tests and shared DML surface contract tests.
- API impact: adds a new public runtime inspection method and expands execution record schema stored under remote `exec/state/*.json`.
