# CLI

The CLI entrypoint is defined in `pyproject.toml` as:

```toml
dml = "daggerml._cli:cli"
```

`src/daggerml/_cli.py` builds the command tree directly from the public `Dml` class and its namespace properties.

## Global behavior

Global flags:

- `-v`, `-vv`, `-vvv`: increase logging verbosity
- `--project-home PATH`
- `--remote-root URI`
- `--user NAME`
- `--config-home PATH`

Success output:

- `None` return values do not print anything to `stdout`.
- Scalar return values print as plain text to `stdout`.
- Collection return values print as compact JSON to `stdout`.
- Root `Dml` classmethod constructors that return `Dml` instances print the new runtime's `status()` payload rather than a serialized runtime object.
- Commands whose return annotation includes `Any` or `Error` may write DML-serialized text to `stdout` via `daggerml._core.dml_dumps` when that serializer wins the runtime type match.
- Union return annotations resolve output by building serializer families from the annotation and choosing the highest-priority family whose allowed subset matches the runtime value.

Failure output:

- Argument parsing errors print usage plus `error: ...` to `stderr` and exit with code `2`.
- `Ctrl+C` exits with code `130`.
- Other exceptions are logged and then printed as `error: ...` on `stderr`.

Input parsing rules:

- Required parameters become positional arguments.
- Parameters with defaults become `--kebab-case` options.
- Boolean options become `--flag` or `--no-flag`.
- Root classmethod parameters that have the same name and resolved type as constructor parameters are supplied through the global constructor option, not repeated on the command.
- Generated union parameters do not expose `--<name>-type` or typed union option variants.
- `Any` and `Error` inputs use DML file/stdin transport. Pass a file path, or `-` for `stdin`.
- `list[...]`, `dict[...]`, and `TypedDict` inputs use JSON file/stdin transport. Pass a file path, or `-` for `stdin`.
- `Ref` inputs are constructed from strings.
- When a parameter allows `None`, the token `null` is preserved as `None`.
- When `str` is one of the allowed scalar types for a parameter, normal scalar tokens stay strings unless a higher-priority non-scalar family matches first.
- Variadic `*args: T` parameters become repeated positional arguments parsed as `T`, using `nargs="*"`.

Generated value parsing uses one ordered serde model per parameter:

1. `None`
2. `Any` / `Error`
3. collections
4. `str` when present
5. remaining scalar constructors

Within that order, the CLI derives `parser -> allowed type subset` from the annotation and accepts the first parsed value whose runtime type matches that parser's subset.

## Top-level commands

Generated directly from public `Dml` methods:

- `dml init`
- `dml status`
- `dml log`
- `dml show`
- `dml diff`
- `dml checkout`
- `dml fetch`
- `dml pull`
- `dml push`
- `dml merge`
- `dml revert`

Common examples:

```bash
dml init --project-home .
dml --remote-root s3://my-bucket/demo init --project-home .
dml status
dml branch create feature --revision HEAD~1
dml tag create v1 --revision @release
dml fetch dml://alice/demo#main
dml push --revision @old-tag --delete
dml log HEAD --limit 5
dml show HEAD
dml diff
dml diff --revision HEAD --relative-to HEAD~1
```

## `branch` namespace

Generated from `Dml.branch`:

- `dml branch list`
- `dml branch create NAME [REVISION]`
- `dml branch move NAME REVISION`
- `dml branch rename OLD NEW`
- `dml branch delete NAME`

Examples:

```bash
dml branch list
dml branch create feature --revision HEAD~1
dml branch create review --revision dml://alice/demo#main
dml branch rename feature trunk
dml branch delete review
```

## `tag` namespace

Generated from `Dml.tag`:

- `dml tag list`
- `dml tag create NAME [REVISION]`
- `dml tag delete NAME`

Examples:

```bash
dml tag list
dml tag create v1
dml tag create v1.1 --revision HEAD~1
dml tag delete v1
```

## `config` namespace

Generated from `Dml.config`:

- `dml config set KEY VALUE [--scope local|global]`
- `dml config show [--contrib]`

Examples:

```bash
dml config set remote.root s3://my-bucket/demo
dml config show
```

## `dag` namespace

Generated from `Dml.dag`:

Use this namespace for committed DAG history and inspection, not for staging new DAGs from the shell.

## `runtime` namespace

Generated from `Dml.runtime`:

- `create`
- `get-node`
- `get-argv`
- `put-literal`
- `put-import`
- `set-node-name`
- `start-fn`
- `commit`
- `list`
- `describe`
- `cancel`

This is the low-level mutable staging surface behind `daggerml.api.new()` and `Dag`.

## `admin` namespace

Generated from `Dml.admin` and nested namespaces:

- `dml admin remote list|gc`
- `dml admin gc [--dry-run]`

Examples:

```bash
dml runtime list
dml admin remote list
dml admin gc --dry-run
```

## CLI-only limits

The CLI is generated from typed Python signatures, so it only exposes argument shapes that can be represented at the command line.

Not available through `dml`:

- passing live Python callables
- `@api.funkify`-style workflows
- arbitrary in-process Python object serialization

Use the Python API for those flows.

## Related pages

- [Python API](python-api.md)
- [Configuration](configuration.md)
- [Errors](errors.md)
