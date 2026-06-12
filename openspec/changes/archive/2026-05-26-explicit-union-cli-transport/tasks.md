## 1. Transport maps

- [ ] 1.1 Add `src/daggerml/_cli.py` maps for type -> serializer, short name -> deserializer function, and type -> short deserializer name.
- [ ] 1.2 Move existing leaf parsing/serialization behavior behind those maps instead of keeping ad hoc type-specific branches.
- [ ] 1.3 Register transports for `str`, `Ref`, `Any`, `Error`, `dict`, `list`, `TypedDict` families, `int`, `float`, `bool`, and `Literal[...]` families exactly as specified in the design.

## 2. Generic union generation

- [ ] 2.1 Refactor `MethodCLI._parser_for()` so it handles only non-union transport lookup and no longer performs implicit union inference.
- [ ] 2.2 Extend `MethodCLI._add_callable_args()` so input union kwargs inspect distinct non-`None` short deserializer names and generate typed mutually exclusive flags only when more than one distinct name is present.
- [ ] 2.3 Extend `MethodCLI._add_callable_args()` so positional input unions inspect distinct non-`None` short deserializer names, add `--<name>-type` only when more than one distinct name is present, and default omission to the first non-`None` union member in annotation order.
- [ ] 2.4 Update generated-command filtering so input union-bearing methods are exposed when every non-`None` member has a mapped short-name entry and mapped short-name deserializer, and omitted otherwise.

## 3. Return serialization and compatibility break

- [ ] 3.1 Update `MethodCLI.run()` to normalize positional union arguments after `argparse` parsing, converting the raw positional token through the selected deserializer or the first non-`None` union member transport when no selector is provided.
- [ ] 3.2 Remove the old inferred union parsing path and ensure input union execution uses mapped transport defaults instead of token-shape inference.
- [ ] 3.3 Update return serialization so union-annotated returns use the serializer for the first non-`None` member in annotation order.
- [ ] 3.4 Make first-member union return serialization fail with a normalized CLI serialization error when the selected serializer cannot encode the actual runtime value, with no fallback to later union members.
- [ ] 3.5 Confirm that affected commands preserve their existing error payload shape and only change successful output serialization where explicitly required by the new serializer-selection rule.

## 4. Tests and docs

- [ ] 4.1 Update `tests/contracts/internal/cli/test_method_cli_contract.py` to cover map-backed typed input union flags, single-name input union collapse, optional positional selectors with first-member defaults, selector-driven conversion, and parse failures for conflicting typed flags.
- [ ] 4.2 Add regression coverage for existing public union-bearing commands, including `dml dag get`, `dml dag describe-node`, `dml dag get-node`, and `dml runtime commit`.
- [ ] 4.3 Add coverage for union-annotated return serialization using the first non-`None` member's serializer.
- [ ] 4.4 Add coverage that a union return whose runtime value is incompatible with the selected first-member serializer fails with a normalized serialization error.
- [ ] 4.5 Add coverage for mixed shared-name input unions such as `Any | Error | Ref` or `dict | TypedDictX | Ref`, including deduped typed flags/selectors and first-member positional defaults.
- [ ] 4.6 Add coverage for non-generatable input unions caused by missing type->name entries or missing short-name deserializers.
- [ ] 4.7 Update `docs/reference/cli.md` to document transport short names, positional selector defaults, typed kwarg flags for multi-name input unions, union return serializer ordering, serialization-failure behavior, and DML/JSON payload expectations.
