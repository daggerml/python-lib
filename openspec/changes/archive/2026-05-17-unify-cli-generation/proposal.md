## Why

The current CLI is spread across many `_cli/*` modules that manually restate the public `Dml` surface. That duplication makes the CLI harder to evolve, leaves some public `Dml` workflows unavailable from the CLI, and weakens the value of the existing signature/docstring/annotation work on `Dml`.

## What Changes

- **BREAKING** replace the `_cli/` command package with a single generated `src/daggerml/_cli.py` entrypoint.
- Generate the CLI from the public `Dml` class and its public namespaces, using signatures, docstrings, and `Annotated` metadata for command structure and help text.
- Expose all public CLI-generatable `Dml` workflows, including runtime/admin/dag/config methods, instead of a curated subset.
- Standardize generated argument rules: required parameters become positional arguments, defaulted parameters become options, boolean defaults preserve current behavior through `--flag` or `--no-flag`, and option names use kebab case.
- Standardize CLI I/O around JSON serialization and structured JSON errors for all commands.
- Remove injectable `s3_client` parameters from public `Dml` sync method signatures, initialize `Dml._s3_client` during construction, and route sync workflows through that stored client instead.
- Document that overload ambiguity is out of scope for this change: the generator will use one runtime-visible signature when multiple overloads exist.
- Document and enforce that methods with non-CLI-generatable parameter types such as `Any` are omitted from generated CLI exposure.

## Capabilities

### New Capabilities
- `generated-dml-cli`: Generate the CLI directly from the public `Dml` surface, including type-driven argument parsing, help generation, method filtering, and JSON serialization rules.

### Modified Capabilities
- `unified-dml-surface`: Adjust the shared `Dml` surface contract so sync workflows use an instance-owned S3 client rather than public `s3_client` parameters, while remaining introspection-ready for CLI generation.
- `shared-internal-configuration`: Remove the documented assumption that some public `Dml` workflows stay CLI-unavailable due to serialization limits when their public parameter types are CLI-generatable.
- `cli-thin-interface`: Redefine the CLI transport layer around one generated entrypoint that exposes the public CLI-generatable `Dml` surface while remaining transport-only.

## Impact

- Affected code: `src/daggerml/_cli/**`, `src/daggerml/_internal/dml.py`, CLI tests, packaging entrypoints, and CLI docs/specs.
- Public interface: the `dml` CLI grammar and help output change broadly; all commands return JSON.
- Runtime behavior: remote sync workflows continue to work after moving S3 client ownership into `Dml` instances.
