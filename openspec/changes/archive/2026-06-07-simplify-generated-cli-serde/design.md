## Context

`src/daggerml/_cli.py` already derives the public `dml` command tree from `Dml`, its namespace properties, runtime annotations, and docstrings. The current complexity is concentrated in union transport handling: explicit typed flags and selectors, post-parse union normalization, and return serializer selection by first union member order.

This change keeps the generated-command model and the thin CLI boundary, but replaces explicit union transport grammar with one ordered serde-priority system. The user-facing goal is fewer CLI forms. The implementation goal is one transport model that works for both inputs and outputs without adding domain logic to the CLI layer.

Constraints:

- The CLI must continue to be derived from `Dml`.
- Argument descriptions still come from `Annotated[...]` metadata.
- Command and subcommand help still come from docstrings.
- `None` results still print nothing.
- The CLI may use runtime value checks for transport matching, but it must not reimplement domain semantics.

## Goals / Non-Goals

**Goals:**

- Remove explicit union transport grammar such as `--value-type` and typed union option variants.
- Define one global serde priority order for generated CLI transport: `None`, `Any/Error`, collections, `float`, `int`, `bool`, `str`, `Ref`.
- Derive input parsing from `parser -> allowed type subset` maps built from each parameter annotation.
- Derive output serialization from `serializer -> allowed type subset` maps built from each return annotation.
- Keep the generated CLI transport-only and preserve the existing help/doc source rules.

**Non-Goals:**

- Changing the `Dml` public method surface.
- Adding hand-maintained per-command CLI logic.
- Inventing new domain selector rules beyond current `Dml` behavior.
- Preserving the old explicit union selector syntax.

## Decisions

### One global parser/serializer priority order owns union handling

The generated CLI will use one shared family order, with one parsing refinement for string-bearing scalar unions.

Base family order:

1. `None`
2. `Any` / `Error`
3. collections
4. `float`
5. `int`
6. `bool`
7. `str`
8. `Ref`

Parsing refinement:

- after `None`, `Any/Error`, and collections,
- if `str` is in the allowed subset for that parameter, try `str` before the remaining scalar constructors.

Rationale:

- This matches the intended CLI behavior discussed for ambiguous unions, including `str | int | None -> str` for non-null scalar tokens.
- It makes union handling deterministic without extra user syntax.
- It keeps `Any`/`Error` and JSON-backed collection transport available ahead of scalar fallback when those types are explicitly allowed.

Alternative considered: first-union-member order. Rejected because the user explicitly wants one global priority order that applies across different annotations.

### Each annotation compiles to parser and serializer subset maps

For each generated parameter or return type, the CLI will derive a map from transport family to the subset of allowed types reachable through that family.

Examples:

- `Ref | str` -> `str -> {str}`, `ref -> {Ref}`
- `Ref | Error` -> `dml -> {Error}`, `ref -> {Ref}`
- `Any | Error | Ref` -> `dml -> {Any, Error}`, `ref -> {Ref}`
- `list[Ref] | Ref` -> `json -> {list[Ref]}`, `ref -> {Ref}`

Parsing then tries each parser in priority order and accepts the first parsed value that matches that parser's allowed subset for the parameter. Serialization picks the highest-priority serializer whose subset matches the runtime value.

Rationale:

- This preserves one CLI form per argument while still honoring the annotation.
- It separates transport-family selection from annotation membership checks.
- It gives `Any` and collection unions a principled fallback story.

Alternative considered: flat type-to-parser dispatch with no subset validation. Rejected because shared families such as `dml` and `json` must still prove that the parsed runtime value matches the specific members allowed by that annotation.

### `None` remains explicit and output-silent

Optional parameters and returns keep a dedicated `None` slot at the top of the priority list, but `None` is only accepted when the parsed value is actually `None`. Successful `None` returns still produce no stdout output.

Rationale:

- This preserves null semantics without making empty strings or missing values ambiguous.
- It keeps the existing no-output behavior for `None` returns.

Alternative considered: treat `None` only as a transport absence case. Rejected because unions such as `str | int | None` need null preservation as a real value.

### Scalar parsing keeps existing bool-flag ergonomics for defaulted booleans

Defaulted boolean kwargs will continue to use `--flag` and `--no-flag` generation. Ordered scalar parsing applies to value-carrying parameters and return transport selection, not to removing the existing bool-flag grammar.

Rationale:

- This avoids regressing established CLI ergonomics.
- It keeps bool transport priority relevant only where a value is actually parsed or serialized.

Alternative considered: force all bool handling through textual scalar parsing. Rejected because it would make common flags worse without helping the new union model.

### Runtime value matching replaces first-member output selection

For union-annotated returns, the CLI will not choose a serializer from annotation order alone. Instead it will build the same `serializer -> allowed type subset` map, walk serializers in global priority order, and pick the first serializer whose subset matches the runtime value.

Rationale:

- This avoids mis-serializing values such as `Error` under `Ref | Error` or `Any | Error | Ref`.
- It makes successful output transport symmetric with input parsing.

Alternative considered: preserve first-member output serializer selection. Rejected because it is both noisier to reason about and incompatible with the desired runtime-type-based transport model.

### The older explicit-union transport direction is superseded

This design replaces the archived explicit-union transport model rather than extending it. Affected commands lose typed selectors and typed option variants in favor of one generated input form.

Rationale:

- The two designs point in opposite directions.
- Keeping both would make the CLI rules incoherent.

## Risks / Trade-offs

- [Priority order can make some lower-priority union members effectively unreachable in common cases] -> Document the order clearly and cover representative unions in contract tests.
- [Shared parser families require runtime subset checks that can be subtle] -> Centralize family matching helpers and test `Any/Error`, collection/scalar, and optional unions directly.
- [Removing explicit selectors is a breaking CLI change] -> Update CLI docs and add regression tests for affected public commands in the same change.
- [Runtime value matching for output adds transport logic to serialization] -> Keep matching purely type/subset based and avoid any domain-specific branching.

## Migration Plan

1. Replace the current union-selector generation and post-parse normalization with subset-map construction and ordered parser execution.
2. Replace first-member union output serialization with ordered runtime subset matching.
3. Update generated CLI docs and contract tests to use the new single-form grammar.
4. Remove the old explicit selector paths entirely; no compatibility shim is planned.

Rollback strategy:

- Revert the change set to restore the previous explicit union transport behavior. No data migration is involved.

## Open Questions

- None.
