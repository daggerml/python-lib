## 1. Parser and serializer priority model

- [x] 1.1 Replace explicit union transport naming and selector helpers in `src/daggerml/_cli.py` with one global serde-priority definition: `None`, `Any/Error`, collections, `float`, `int`, `bool`, `str`, `Ref`.
- [x] 1.2 Implement annotation-to-transport compilation that builds `parser -> allowed type subset` and `serializer -> allowed type subset` maps for single types, unions, optionals, collections, and `Any/Error` families.
- [x] 1.3 Preserve existing bool-flag generation, annotation-derived argument descriptions, and docstring-derived command help while switching value-carrying parameters to the new ordered parsing path.

## 2. Generated CLI execution changes

- [x] 2.1 Remove generated CLI union grammar such as `--<name>-type` and typed union option variants, and make affected commands use one generated argument form per parameter.
- [x] 2.2 Update command invocation so input parsing tries parser families in priority order and accepts only values matching that parser's allowed subset for the parameter.
- [x] 2.3 Update result serialization so union-annotated returns choose the highest-priority serializer whose allowed subset matches the runtime value, while preserving no output for `None` results.

## 3. Tests and docs

- [x] 3.1 Add or update CLI contract tests for representative unions including `Ref | str`, `str | int | None`, `Ref | Error`, `Any | Error | Ref`, and collection-or-ref parameters.
- [x] 3.2 Add regression coverage for affected public commands such as `dml checkout`, `dml dag get`, `dml config set`, and `dml runtime commit` to confirm the new single-form grammar and serializer behavior.
- [x] 3.3 Update `docs/reference/cli.md` to document the ordered serde model, the removal of explicit union selectors, file/stdin behavior for DML and JSON transport, and the unchanged `None` output rule.
