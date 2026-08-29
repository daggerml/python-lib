## Why

The generated `dml` CLI currently exposes explicit union transport grammar such as `--value-type` and typed option variants, which makes the surface noisy and pushes transport-selection decisions onto users. We want a simpler CLI that still derives from `Dml`, but resolves parsing and serialization through one ordered serde model per argument and return value.

## What Changes

- Remove explicit union transport grammar such as `--<name>-type`, `--<name>-str`, and `--<name>-ref` from generated `dml` commands.
- Define one ordered serde-priority model for generated CLI inputs: `None`, `Any/Error`, collections, `float`, `int`, `bool`, `str`, then `Ref`.
- Build generated input parsing from `parser -> allowed type subset` maps derived from each annotation, trying parsers in priority order and accepting the first parsed value that matches that parser's allowed subset for the parameter.
- Build generated output serialization from `serializer -> allowed type subset` maps derived from each return annotation, selecting the highest-priority serializer whose subset matches the runtime value.
- Preserve the existing `Dml`-derived command tree, annotation-derived argument descriptions, and docstring-derived command help.
- Preserve the rule that `None` results do not print anything.
- **BREAKING** Replace explicit union-type CLI syntax with single-form parsing for affected commands such as `checkout`, `config set`, and `runtime commit`.
- **BREAKING** Change union return serialization to choose the serializer by runtime value compatibility within the annotated type subset instead of by first union member order.

## Capabilities

### New Capabilities

None.

### Modified Capabilities

- `generated-dml-cli`: Generated argument parsing and result serialization change from explicit union transport grammar to ordered parser/serializer subset matching.
- `repo-inspection-cli`: Repository-facing commands such as `dml checkout` and `dml dag get` lose explicit union transport selectors and rely on ordered single-form parsing.
- `cli-thin-interface`: The CLI transport layer remains thin, but its union handling contract changes from selector-driven transport choice to ordered parser/serializer subset matching.

## Impact

- Affected code: `src/daggerml/_cli.py` and CLI contract tests.
- Affected docs: `docs/reference/cli.md`.
- Affected public surface: generated `dml` command grammar and successful output serialization for union-annotated commands.
- No new runtime dependencies are required.
