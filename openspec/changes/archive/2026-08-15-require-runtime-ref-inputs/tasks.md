## 1. Runtime Ref Contract

- [x] 1.1 Add a focused runtime-ref validator that rejects non-`Ref` values and wrong namespaces without requiring local DB dereference, with `index` required for creation and supported runtime namespaces allowed for inspection and cancellation.
- [x] 1.2 Change `Dml.runtime.create` from `execution_id: str | None` to `execution: Ref | None`, preserve cache/execution pairing validation, and delegate `execution.id()` to `IndexOps.create`.
- [x] 1.3 Change `Dml.runtime.read_launch_state` and `read_execution_record` to require `Ref`, validate it, and delegate only its extracted execution ID.
- [x] 1.4 Change `Dml.runtime.describe_graph` roots to `Ref` only while preserving no-root discovery and visual rendering behavior.
- [x] 1.5 Change `Dml.runtime.cancel` to require `Ref`, remove all string-to-`Ref` coercion, delegate the extracted ID, and preserve the requested ref in the cancellation summary.

## 2. Caller Migration

- [x] 2.1 Update the high-level API execution-aware runtime creation path to construct or pass an `index` `Ref` while retaining string execution IDs in adapter and execution-state protocols.
- [x] 2.2 Update the contrib supervisor and its contract fixture to establish an `index` `Ref` before calling execution-record inspection or drive-mode cancellation.
- [x] 2.3 Audit remaining source callers of the five narrowed runtime methods and migrate every Dml-bound execution identity to `Ref` without changing lower-level `ExecutionState` string-ID calls.

## 3. Contract And CLI Tests

- [x] 3.1 Add Dml contract coverage for ordinary and execution-aware creation with refs, paired-input validation, wrong namespaces, bare IDs, and ref-shaped string rejection.
- [x] 3.2 Replace string-success tests for launch-state and execution-record reads with ref-success cases and add explicit pre-delegation rejection tests for both bare and ref-shaped strings.
- [x] 3.3 Add graph and cancellation tests proving active and frozen refs retain behavior while string roots and targets are rejected before lower-level side effects.
- [x] 3.4 Update generated CLI tests to pass canonical `index:<id>` or `frozenindex:<id>` tokens, and verify exact `Ref` annotations provide conversion without command-specific CLI parsing.
- [x] 3.5 Update API and contrib tests affected by the `runtime.create` keyword rename and strict runtime identity boundary.

## 4. Documentation

- [x] 4.1 Update runtime reference, inspection/cancellation guide, and runtime concept docs to show canonical ref text at the CLI and `Ref` values in Python.
- [x] 4.2 Update execution/runtime and public API/CLI architecture docs to distinguish public `Dml.runtime` refs from lower-level string execution IDs and state that CLI adaptation does not widen Dml signatures.
- [x] 4.3 Check generated help and other runtime examples for bare execution IDs passed to affected Dml commands, updating them while leaving revision selector documentation unchanged.

## 5. Verification

- [x] 5.1 Run focused Dml runtime, generated CLI, API, and contrib supervisor contract tests.
- [x] 5.2 Run lint/type-oriented checks used by the repository and fix strict-signature fallout without adding compatibility coercion.
- [x] 5.3 Run the full test suite and confirm lower-level execution-state persistence, adapter envelopes, and revision selector behavior remain unchanged.
