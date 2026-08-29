## Why

The current CLI layer appears to include behavior beyond argument parsing and output serialization, which makes it harder to test, reason about, and evolve interface contracts safely. We need a clear boundary now to improve maintainability and keep business rules centralized in core modules.

## What Changes

- Refactor the CLI surface so command handlers only parse inputs, invoke domain APIs, and serialize outputs.
- Move decision-making and workflow logic currently in CLI command paths into appropriate internal/public API layers.
- Standardize CLI command result shaping so output formatting is consistent and transport-focused.
- Remove or simplify CLI-only branching that duplicates domain behavior.
- Preserve existing user-visible command semantics unless a compatibility adjustment is explicitly required.

## Capabilities

### New Capabilities
- `cli-thin-interface`: Define and enforce CLI responsibility boundaries for input parsing and output serialization only.

### Modified Capabilities


## Impact

- Affected code: `src/daggerml/_cli/**` and any modules currently called from CLI that will absorb moved logic.
- APIs: CLI command internals and invocation paths; no intentional public CLI UX breakage.
- Tests: CLI tests and potentially API/internal tests updated to assert shifted responsibility.
- Systems: Improves separation of concerns between interface and domain layers, reducing duplicate logic.
