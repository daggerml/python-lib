## Why

Adapter execution currently tolerates a runtime fallback that imports Python adapter objects when a concrete adapter command is missing from `PATH`. That blurs the boundary between adapter-name sugar and concrete runtime execution, and it allows invalid resolved runnables to keep working instead of failing as installation or configuration errors.

## What Changes

- Define a strict runtime invariant: any runnable that reaches adapter execution MUST carry a command-line-callable adapter string or an explicit filesystem path.
- Preserve symbolic adapter names such as `local` and `lambda` only as authoring-time sugar that is resolved before runtime execution.
- Treat built-in adapters `dml-local-adapter` and `dml-lambda-adapter` as canonical concrete adapter identities.
- Support plugin-defined sugar that resolves to any concrete callable adapter string, including names that do not start with `dml-` and explicit executable paths.
- Reserve `adapter == ""` only for explicit builtin-function execution paths that never shell out to an adapter.
- **BREAKING** Remove runtime fallback behavior that imports adapter specs and invokes `cli()` when the concrete adapter command is not present on `PATH`.

## Capabilities

### New Capabilities

- `adapter-cli-resolution`: defines how symbolic adapter names resolve to concrete command-line adapter identities and how runtime execution handles builtin exceptions and missing commands.

### Modified Capabilities

None.

## Impact

- Affected code: delayed runnable normalization, adapter registry usage, `IndexOps` adapter invocation, contrib executor runnable construction, and adapter-path test helpers.
- Affected runtime behavior: missing concrete adapter commands become hard failures instead of falling back to Python import-based execution.
- Affected plugin contracts: plugin adapters may continue to provide sugar, but they must resolve to a concrete command-line-callable adapter identity before runtime execution.
- Affected test fixtures: adapter-backed test helpers such as `tests/assets/internal_fn/python-fork-adapter.py` must be executable from the command line when referenced as runtime adapters, because runtime execution no longer repairs non-executable adapter references via Python import fallback.
