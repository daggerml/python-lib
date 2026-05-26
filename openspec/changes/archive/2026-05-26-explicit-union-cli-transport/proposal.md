## Why

The generated `dml` CLI currently treats non-`None` union annotations as parser special cases instead of routing every member type through declared transports. That is ambiguous for commands such as `dml dag get`, and it does not scale cleanly to existing union-bearing signatures such as `runtime commit(value: Ref | Error)`.

The change needs to be fully specified as one breaking redesign: every generated parameter type must have explicit CLI serialization and deserialization behavior, and union construction plus union return serialization must be derived from transport metadata rather than from hard-coded type-specific branches.

## What Changes

- Define three authoritative maps in `src/daggerml/_cli.py`:
  - annotation/runtime type -> CLI serializer
  - short transport name -> CLI deserializer function
  - annotation/runtime type -> short transport name
- Move existing leaf-type handling behind those maps, including `Ref`, scalar types, JSON-backed container types, `Any`, and `Error`.
- Define union input CLI generation generically by mapping each non-`None` union member to its short deserializer name.
- If an input union collapses to a single distinct deserializer short name, use that transport directly without adding typed union flags.
- If an input union has multiple distinct deserializer short names, generate typed union grammar from those names.
- For defaulted or required-as-option input union parameters with multiple distinct names, generate mutually exclusive flags `--<arg>-<name>` with one flag per distinct deserializer name.
- For positional input union parameters with multiple distinct names, generate an optional `--<arg>-type` selector whose choices are the distinct deserializer names and whose omission defaults to the first non-`None` union member in annotation order.
- Determine union return serialization similarly from the serializer map, but use only the first non-`None` member type in annotation order when the return annotation is a union.
- **BREAKING** Remove the old implicit union inference behavior entirely.
- **BREAKING** Replace old untyped option forms for affected union kwargs, such as `--dag`, with explicit typed variants such as `--dag-str` and `--dag-ref` when an input union resolves to multiple deserializer names.
- Keep the redesign transport-only: the CLI chooses serializers/deserializers, invokes the target method, and leaves domain semantics to `Dml`.
- Update generated CLI specs, repo-inspection CLI spec, thin-interface spec, user CLI docs, and contract tests to match the new grammar.

## Capabilities

### New Capabilities

- `generated-dml-cli`: Union generation works generically from deserializer short names rather than hard-coded union-member cases.

### Modified Capabilities

- `repo-inspection-cli`: DAG inspection commands use explicit transport selection instead of inference when an input union maps to multiple deserializer names.
- `cli-thin-interface`: Generated CLI transport is defined by serializer/deserializer maps rather than ad hoc parser branches.

## Impact

- Affected code: `src/daggerml/_cli.py` and generated CLI tests under `tests/contracts/internal/cli/`.
- Affected docs: `docs/reference/cli.md`.
- Affected public surface: generated `dml` command grammar for union-annotated parameters and serializer selection for union-annotated returns, including DAG inspection commands and any union-bearing generated commands such as `runtime commit`.
- No new runtime dependencies are required.
