## Context

`src/daggerml/_cli.py` builds the `dml` command tree dynamically from the public `Dml` type, its public methods, and its `@property` namespace accessors. Today the generator already discovers leaf commands before namespaces, but both categories are registered on one `argparse` subparser action, so help output collapses them into a single positional-arguments section.

The same root parser also owns global flags such as `-v` and constructor-derived options. Package version metadata already exists in `daggerml.__about__`, but the generated CLI does not expose a conventional root `--version` action.

Constraints:

- The CLI must remain generated from runtime-visible `Dml` signatures and namespace properties.
- Parsing behavior and command routing should not change for existing commands.
- Help and version output must preserve the current color behavior instead of bypassing `argparse` with ad hoc printing.

## Goals / Non-Goals

**Goals:**

- Add a conventional root `--version` flag for `dml`.
- Render generated help with commands first and namespaces in a separate `namespaces` section.
- Keep the generated parser tree and dispatch flow intact.
- Preserve current color and terminal-sensitive output behavior.
- Limit implementation scope to the CLI generator, tests, and user-facing CLI docs.

**Non-Goals:**

- Do not hand-maintain per-command help text or command registration.
- Do not change command names, command routing, argument parsing, or namespace discovery rules.
- Do not introduce a new help library or external dependency.
- Do not change non-help stdout serialization for normal command results.

## Decisions

### Keep one real subparser action and split categories only in help rendering

The generated CLI will continue to register commands and namespaces on one real `argparse` subparser action. The visual split between commands and namespaces will be introduced by attaching category metadata to generated entries and teaching the help-rendering path to display them in two sections.

Rationale:

- `argparse` only supports one subparser action per parser, so a parsing-level split would fight the library instead of using it.
- The existing command tree and dispatch model already work; the problem is presentation.
- A rendering-only split minimizes risk to parsing behavior.

Alternative considered: create separate parser groups for commands and namespaces. Rejected because that would require replacing the normal subparser grammar or emulating it with custom parsing behavior.

### Use argparse-managed version output instead of custom printing

The root parser will expose `--version` through the parser action layer and source the displayed version string from the package version metadata already published by the project.

Rationale:

- This matches expected CLI behavior such as `dml, version <version>`.
- It keeps exit handling, stdout routing, and parser-owned presentation consistent with the rest of the CLI.

Alternative considered: intercept `argv` manually and print a version string before parser creation. Rejected because it duplicates parser behavior and is more likely to drift from parser-managed formatting behavior.

### Preserve the existing formatter and parser output path as much as possible

The implementation should extend the current `argparse` help path just enough to separate command and namespace listings, while continuing to use parser-managed formatting and output methods.

Rationale:

- The current request explicitly wants color behavior preserved.
- Replacing help with manually assembled text would create avoidable risk around terminal-sensitive formatting.

Alternative considered: override `format_help()` with a hand-built string. Rejected because it is the most likely path to regress formatting or color behavior.

### Apply the same help grouping rules to nested namespace parsers

Any generated parser that exposes both leaf commands and nested namespaces should render leaf commands first and render namespace groups in a distinct `namespaces` section.

Rationale:

- The same scanning problem appears on nested parsers such as `dml admin`.
- One consistent grouping rule is easier to understand and test than a root-only exception.

Alternative considered: limit the split to the root parser. Rejected because it would leave nested namespace help inconsistent.

## Risks / Trade-offs

- [Argparse help customization can be brittle across Python versions] -> Keep the customization narrow, rely on existing parser machinery, and cover root plus nested namespace help in contract tests.
- [Help grouping metadata could leak into parsing behavior] -> Keep category labels help-only and avoid using them in dispatch or argument resolution.
- [Version output format could drift from user expectation] -> Pin the exact public string format in a contract test.
- [Preserving color behavior is hard to prove in non-interactive tests] -> Avoid manual printing paths and keep help/version output on parser-managed code paths.

## Migration Plan

1. Extend the CLI generator with a parser-managed root `--version` action.
2. Add help-grouping metadata for generated commands versus namespaces.
3. Update help rendering so any mixed parser shows `commands` first and `namespaces` second.
4. Update CLI contract tests and `docs/reference/cli.md` to reflect the new public surface.

Rollback strategy:

- Revert the CLI generator and test/doc updates. No stored data or migration state is involved.

## Open Questions

- None.
