## Why

The generated `dml` CLI currently mixes leaf commands and namespace groups into one subparser help section, which makes the top-level surface harder to scan. It also lacks a standard `--version` flag even though the package already publishes version metadata.

## What Changes

- Add a root `--version` flag that prints a conventional CLI version string for `dml` and exits successfully.
- Change generated help output so leaf commands are listed before namespace groups.
- Label namespace groups under a distinct `namespaces` help section instead of mixing them into the main command list.
- Preserve the current color behavior for generated help and version output.
- Preserve the existing generated command tree and parsing behavior; this change is about presentation, not command routing.

## Capabilities

### New Capabilities

- None.

### Modified Capabilities

- `generated-dml-cli`: generated root flags and help rendering gain a `--version` surface plus separate command and namespace sections.

## Impact

- Affected code: `src/daggerml/_cli.py`.
- Affected tests: generated CLI contract tests covering root help and global flags.
- Affected docs: `docs/reference/cli.md` should document `--version` and the command-versus-namespace help layout.
- No dependency, storage, or runtime execution impact.
