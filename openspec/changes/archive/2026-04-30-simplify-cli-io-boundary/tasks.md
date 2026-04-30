## 1. Baseline and boundary definition

- [x] 1.1 Inventory `src/daggerml/_cli/**` command handlers and list locations where domain/workflow logic currently exists.
- [x] 1.2 Define per-command domain entrypoints (API or internal ops) that CLI handlers should delegate to.
- [x] 1.3 Capture baseline CLI behavior for critical commands (output shape, exit outcomes, key error cases) with regression tests.

## 2. Extract CLI orchestration logic

- [x] 2.1 Refactor command handlers to keep only input parsing, delegation, and output serialization.
- [x] 2.2 Move extracted branching/workflow decisions into appropriate API/internal modules with explicit interfaces.
- [x] 2.3 Remove CLI-local duplicated decision branches once equivalent domain logic paths are validated.

## 3. Normalize output and error handling

- [x] 3.1 Introduce or align structured command result envelopes used by CLI formatters.
- [x] 3.2 Ensure CLI preserves existing externally visible success output and failure signaling semantics.
- [x] 3.3 Add/update tests for consistent serialization and exit signaling across representative commands.

## 4. Final verification and guardrails

- [x] 4.1 Shift behavior-heavy assertions to API/internal tests and keep CLI tests focused on transport concerns.
- [x] 4.2 Add lightweight guardrails (review checklist or lint/test pattern) to prevent new business logic from being added in CLI modules.
- [x] 4.3 Run targeted test suites for CLI and touched domain layers, and fix any regressions before merge.
