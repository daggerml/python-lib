## Why

The shared `Dml` surface is now the canonical orchestration boundary for CLI and API workflows, but its runtime introspection story is still sparse: public methods and namespace objects largely lack docstrings, and parameter intent is not captured in machine-readable form. We want to make the `Dml` surface self-describing now so future tooling can programmatically derive CLI help and related introspection without redefining command semantics in `_cli`.

## What Changes

- Add class docstrings to the public `Dml` class and the namespace objects reachable from it so introspection can describe the purpose of each command group.
- Add method docstrings throughout the public `Dml` surface, including namespaced methods, so introspection can describe operation behavior, constraints, and side effects.
- Add `typing.Annotated` metadata to user-facing `Dml` and namespace method parameters so parameter meaning is available as structured help text for future CLI generation and related tooling.
- Define the documentation split for this surface: signature defaults remain the source of truth for default values, `Annotated` metadata documents parameter meaning, and docstrings document class/namespace purpose plus method behavior.
- Keep runtime behavior, CLI grammar, and output payloads unchanged in this change.

## Capabilities

### New Capabilities

None.

### Modified Capabilities

- `unified-dml-surface`: Add introspection-oriented documentation and parameter metadata requirements for the shared `Dml` boundary and its public namespaces.

## Impact

- Affects `src/daggerml/_internal/dml.py` and any public wrappers or tests that assert `Dml` signature and documentation behavior.
- Does not change repository state formats, runtime execution behavior, or current CLI command semantics.
- Prepares the `Dml` surface for future programmatic CLI derivation by making descriptions available directly on classes, methods, and parameters.
