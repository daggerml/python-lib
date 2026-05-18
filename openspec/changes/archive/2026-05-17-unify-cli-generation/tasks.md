## 1. Reshape the `Dml` surface for CLI generation

- [x] 1.1 Remove public `s3_client` parameters from sync-facing `Dml` methods and initialize `Dml._s3_client` during construction.
- [x] 1.2 Update remote sync helpers and callers so `fetch`, `pull`, and `push` use the Dml-owned S3 client.
- [x] 1.3 Review public `Dml` and namespace method annotations/docstrings so generated CLI help and parsing metadata are complete and accurate.

## 2. Build the generated CLI entrypoint

- [x] 2.1 Replace the `_cli/` package implementation with a single `src/daggerml/_cli.py` module that owns parser generation, dispatch, logging, and JSON/error serialization.
- [x] 2.2 Implement command discovery from public `Dml` methods, public namespaces, and supported class entrypoints such as `Dml.init`.
- [x] 2.3 Implement type-driven argument generation and parsing for supported types, including boolean flag inversion rules, `Ref` parsing, JSON-backed container parsing, and positional help text rendering.
- [x] 2.4 Implement method filtering so commands are generated only for methods whose public parameter types are CLI-generatable, and document the one-runtime-signature overload rule in code comments or user-facing help where appropriate.

## 3. Update packaging, tests, and docs

- [x] 3.1 Update CLI packaging/import wiring and remove obsolete `_cli/*` command modules.
- [x] 3.2 Add or update CLI tests covering generated command exposure, unsupported-method filtering, JSON output/errors, and representative runtime/admin/dag/config flows.
- [x] 3.3 Update CLI documentation to describe the generated surface, JSON-only output, supported type parsing rules, and the overload limitation.
