## Context

`src/daggerml/_cli.py` currently mixes ordinary leaf parsing with special-case union inference. That creates ambiguity for public DAG inspection commands and does not provide a principled transport for other union-bearing signatures such as `runtime commit(value: Ref | Error)`.

Aaron's required design direction is to stop baking specific union member sets into union construction logic. Instead, the CLI should know how supported types serialize, how short transport names map to deserializers, and how each type maps to its short deserializer name. Union generation should then be assembled generically from those names, and union return serialization should select the serializer from the first non-`None` return member.

## Goals / Non-Goals

**Goals:**

- Make generated CLI transport for non-`None` unions explicit and documented.
- Define one map-based transport system for both ordinary parameters and union members.
- Support generic union construction by deserializer short name instead of hard-coded type combinations.
- Use direct transport when an input union resolves to one distinct deserializer name.
- Use typed union grammar only when an input union resolves to multiple distinct deserializer names.
- Keep positional selector defaults driven by union member order, not token-shape inference.
- Serialize union-annotated returns through the serializer for the first non-`None` union member in annotation order.
- Keep union handling in the CLI transport layer only: parser construction, post-parse conversion, and result serialization.
- Ship this as a breaking change with no compatibility fallback.

**Non-Goals:**

- Preserving the old inferred union behavior.
- Adding domain-specific validation rules to the CLI layer.
- Inventing transport behavior for types that are not represented in the maps.
- Dynamically inferring a return serializer from runtime value shape when a union return annotation is present.

## Decisions

### Transport is defined by three authoritative maps

`src/daggerml/_cli.py` SHALL define three authoritative maps:

1. **Serializer map**: annotation or runtime type -> CLI serializer.
2. **Deserializer-name map**: short transport name -> CLI deserializer function.
3. **Type-to-name map**: annotation or runtime type -> short deserializer name.

Input union construction SHALL consult the type-to-name map and the deserializer-name map rather than branching on specific member combinations. Union return serialization SHALL consult the serializer map using the first non-`None` member type from the return annotation.

Rationale:

- This separates transport naming from transport implementation.
- Ordinary parameters, input unions, and union returns follow the same transport rules.
- New union-capable member types are enabled by registering names and serializers/deserializers instead of rewriting union logic.

### Required built-in transport families

The implementation SHALL register at least the following built-in families and short names:

| Family / annotation shape | Short name | Input deserializer | Output serializer | Notes |
| --- | --- | --- | --- | --- |
| `str` | `str` | raw CLI token | raw string | direct scalar transport |
| `Ref` | `ref` | existing `Ref` parser | `ref.to` | preserves current ref output form |
| `Any` | `dml` | `daggerml._internal.dml_loads` | `daggerml._internal.dml_dumps` | exact `Any` keeps file-backed DML transport; `-` means stdin |
| `Error` | `dml` | `daggerml._internal.dml_loads` | `daggerml._internal.dml_dumps` | shares DML transport with `Any` |
| `dict` | `json` | JSON loads | JSON dumps | structured JSON transport |
| `list` | `json` | JSON loads | JSON dumps | structured JSON transport |
| `TypedDict` families | `json` | JSON loads | JSON dumps | structured JSON transport |
| `int` | `int` | integer parse | integer serializer | existing leaf behavior |
| `float` | `float` | float parse | float serializer | existing leaf behavior |
| `bool` | `bool` | existing bool CLI behavior | bool serializer | existing flag semantics remain |
| `Literal[...]` | matching scalar short name of the literal family | existing literal parse | matching scalar serializer | constrained by literal choices |

No unspecified "other already supported leaf transports" are part of this change. Any additional family added during implementation MUST be specified in docs/specs in the same change.

### Bool and Literal are resolved before generic transport and union handling

During input argument generation and parse setup, the CLI SHALL process `bool` and `Literal[...]` before generic transport-map lookup and before any union-member dispatch logic.

Specifically:

- `bool` SHALL always use the existing flag semantics and SHALL NOT participate in typed union transport generation.
- `Literal[...]` SHALL always use literal-choice parsing derived from the literal family and SHALL NOT participate in typed union transport generation.
- Only after those two cases are ruled out SHALL the CLI apply ordinary transport-map lookup, direct transport selection, and union transport generation.

Rationale:

- This preserves the established flag behavior for booleans.
- This keeps literal parsing as constrained scalar-choice parsing rather than a union transport problem.
- This makes the dispatch order explicit instead of leaving it implicit in implementation structure.

### Input union generation depends on distinct deserializer short names

For each non-`None` input union member, the CLI SHALL look up that member's short deserializer name through the type-to-name map.

Then:

- if the input union yields exactly one distinct short name, the CLI SHALL use that transport directly and SHALL NOT generate typed union flags or selectors for that parameter;
- if the input union yields multiple distinct short names, the CLI SHALL generate explicit typed union grammar from those names.

Examples:

- `Any | Error` -> one distinct name `dml`, so the CLI uses direct DML transport with no extra union selector; when the selected member is exact `Any`, that transport still accepts a file path and treats `-` as stdin.
- `dict | list` -> one distinct name `json`, so the CLI uses direct JSON transport with no extra union selector.
- `str | Ref` -> distinct names `str` and `ref`, so the CLI generates typed union grammar.
- `str | Ref | None` -> distinct names `str` and `ref`, so the CLI generates typed union grammar for non-`None` members.
- `Any | Error | Ref` -> distinct names `dml` and `ref`, so the CLI generates typed union grammar deduped to those two names.

Rationale:

- The CLI only needs extra syntax when the input union actually has more than one possible deserializer choice.
- Shared payload formats stop being a collision problem when they intentionally share one deserializer name.
- This keeps the union logic generic while still producing minimal CLI grammar.

### Union returns use the first non-None member serializer

When a return annotation is a union, the CLI SHALL ignore `None` members and select the serializer associated with the first remaining member type in annotation order.

Examples:

- `str | Ref` return -> serialize as `str` because `str` is first.
- `None | Ref` return -> serialize as `Ref` because `None` is ignored.
- `Any | Error` return -> serialize with the `Any` serializer because `Any` is the first non-`None` member.

If the selected first-member serializer cannot serialize the actual runtime return value, the command SHALL fail with a normalized CLI serialization error. The CLI SHALL NOT fall back to later union members and SHALL NOT inspect runtime value shape to choose a different serializer.

Rationale:

- This matches the user's requested rule and keeps return serialization deterministic.
- It avoids adding output selector syntax or runtime type introspection to the CLI layer.

### Output format is derived from the output type

Successful generated-command output SHALL be derived from the resolved output type and its registered serializer.

Specifically:

- exact `Any` returns SHALL use the DML serializer and print the resulting DML text as-is;
- non-union returns SHALL use the serializer registered for their output type and print that serializer's output as-is;
- union returns SHALL first select the output type by taking the first non-`None` member in annotation order, then use that member's registered serializer, and print that serializer's output as-is.

The CLI SHALL NOT impose a separate global "all successful outputs are JSON" rule. JSON is only one serializer family among the registered output types.

Rationale:

- Output behavior follows the same map-driven transport model as input behavior.
- This makes DML text output, JSON text output, scalar text output, and union-selected output all consequences of output typing rather than command-specific special cases.

### Typed kwarg union grammar is generated from distinct deserializer names

For a defaulted parameter, or for a required parameter emitted as an option because `required_as_options=True`, an input union parameter with multiple distinct short names generates one flag per distinct short name:

- `--<kebab-name>-<short-name>`

All such flags write to one destination and live in one mutually exclusive group. The group is required only when the underlying parameter is required and is already being rendered as an option.

Examples:

- `dag: str | Ref | None = None` -> `--dag-str VALUE` or `--dag-ref VALUE`
- `value: Ref | Error` -> `--value-ref VALUE` or `--value-dml VALUE`
- `payload: dict | TypedDictX | Ref` -> `--payload-json VALUE` or `--payload-ref VALUE`

When multiple union members map to the same short name, they share one generated CLI form because they intentionally share one deserializer.

### Positional union grammar is generated from distinct deserializer names

For a positional input union parameter with multiple distinct short names, the CLI keeps the positional token and adds an optional selector option:

- positional `<name>`
- `--<kebab-name>-type {<names...>}`

The positional token is parsed as raw text first. `MethodCLI.run()` then selects the deserializer named by the selector, or defaults to the first non-`None` union member's short name in annotation order when the selector is omitted, before invoking the target method.

If the input union has only one distinct short name, the CLI SHALL NOT add `--<name>-type`; it SHALL parse directly with that one deserializer.

Examples:

- `dml dag get VALUE` defaults to `str` when `str` is the first non-`None` union member.
- `dml dag get dag:abc123 --value-type ref` explicitly selects `Ref` transport.
- A positional `Any | Error` parameter uses direct `dml` transport with no selector.
- A positional `Any | Error | Ref` parameter exposes `--value-type {dml,ref}` and defaults to `dml` if `Any` is first.

### No backward-compatibility fallback

The redesigned union grammar ships as a breaking change with no inference fallback.

Consequences:

- `dml dag get train` remains valid because union order defaults to `str`, not because of token-shape inference.
- `dml dag get dag:abc123 --value-type ref` remains the explicit way to force `Ref` transport when the default first union member is not `Ref`.
- Affected kwargs lose untyped forms such as `--dag` in favor of typed forms when an input union has multiple distinct deserializer names.

### Generatability is determined by transport-map coverage

An input union-bearing parameter is CLI-generatable when, after excluding `None`, every member has a registered short deserializer name and corresponding deserializer function, and union member order is preserved for first-member defaults.

An input union-bearing parameter is not CLI-generatable when any member lacks:

- a registered short deserializer name, or
- a registered deserializer function for that short name.

A union-annotated return is serializer-resolvable when, after excluding `None`, the first remaining member has a registered serializer.

Rationale:

- The generic rule is map coverage, not hard-coded allowlists.
- Shared names are allowed when they intentionally point to the same deserializer.

## Risks / Trade-offs

- [Breaking existing CLI invocations for union-bearing commands] -> Update `docs/reference/cli.md`, generated CLI specs, and contract tests in the same change.
- [Shared deserializer names collapse some input unions to one CLI transport] -> Document that this is intentional and means those unions do not need extra selector syntax.
- [First-member union return serialization may fail for later-member runtime values] -> Define and test the normalized serialization-error behavior; do not fall back.
- [Using DML or JSON text for some transports moves more payload construction into the shell] -> Keep examples and docs explicit, and preserve transport-only behavior in the CLI layer.
- [Generic post-parse conversion increases parser orchestration complexity] -> Keep the logic map-driven and cover it with contract tests.

## Migration Plan

1. Add the three transport maps and migrate existing leaf parsing and serialization to them.
2. Update input union generation to derive direct-vs-typed grammar from distinct short deserializer names.
3. Update return serialization so union returns use the serializer of the first non-`None` member in annotation order and fail deterministically if that serializer cannot encode the runtime value.
4. Update generated-command filtering so it is based on input transport-map coverage.
5. Update `src/daggerml/_cli.py` and contract tests to implement the new parser and serializer selection rules.
6. Update `docs/reference/cli.md` examples and command descriptions to use typed flags only when an input union has multiple distinct short names and to document positional defaults plus union return serializer ordering.

Rollback strategy:

- Revert the change set to restore the prior inferred grammar; no data migration is involved because this is a parser-surface change only.

## Open Questions

- None.

