## 1. Root version surface

- [x] 1.1 Add a root `--version` action in `src/daggerml/_cli.py` that prints `dml, version <version>` using the package version metadata.
- [x] 1.2 Keep `--version` parser-managed so it exits successfully without requiring a command and does not affect normal command dispatch.

## 2. Help grouping behavior

- [x] 2.1 Extend generated subparser registration so leaf commands and namespace groups carry enough metadata to be rendered separately in help.
- [x] 2.2 Update generated help rendering so mixed parsers show `commands` first and `namespaces` second.
- [x] 2.3 Apply the same grouping rule to nested namespace parsers such as `dml admin` without changing the generated command grammar.
- [x] 2.4 Preserve the current color and formatter behavior by keeping help and version output on parser-managed paths.

## 3. Tests and docs

- [x] 3.1 Add CLI contract coverage for `dml --version` output and successful exit behavior.
- [x] 3.2 Add CLI contract coverage for root help proving commands and namespaces render in separate sections and in that order.
- [x] 3.3 Add nested namespace help coverage proving mixed namespace parsers also split commands from namespaces.
- [x] 3.4 Update `docs/reference/cli.md` to document `--version` and the command-versus-namespace help layout.
- [x] 3.5 Run targeted CLI contract tests and any required CLI validation from `CONTRIBUTING.md`.
- [x] 3.6 Run `openspec status --change improve-generated-cli-help-and-version` and confirm the change is apply-ready.
