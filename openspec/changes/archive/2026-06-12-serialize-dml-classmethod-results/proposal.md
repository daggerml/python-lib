## Why

Root `Dml` bootstrap commands such as `init`, `clone`, and `from-config-vars` return `Dml` instances, but the generated CLI serializer is designed around JSON-ready payloads and typed leaf values. That leaves classmethod commands with a result shape the CLI cannot present consistently, even though the user-facing outcome should be the newly initialized repository status.

## What Changes

- Update generated CLI result handling for root `Dml` classmethod commands that return `Dml` instances.
- Treat returned `Dml` instances as a request to serialize `dml.status()` rather than the raw runtime object.
- Add contract coverage for classmethod bootstrap commands so CLI output stays aligned with the shared `Dml` surface.

## Capabilities

### New Capabilities

### Modified Capabilities
- `generated-dml-cli`: root classmethod commands that return `Dml` instances will serialize repository status instead of attempting to serialize the raw `Dml` object.

## Impact

- Affected code: `src/daggerml/_cli.py` and generated CLI contract tests.
- Affected behavior: CLI output for root `Dml` classmethod bootstrap commands.
- No new dependencies or external systems.
