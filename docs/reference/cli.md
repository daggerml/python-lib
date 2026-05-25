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

- Commands print their return values as formatted JSON to `stdout`.
- Commands whose return annotation is exactly `Any` instead write DML-serialized text to `stdout` via `daggerml._internal.dml_dumps`.

Failure output:

- Argument parsing errors print usage plus `error: ...` to `stderr` and exit with code `2`.
- `Ctrl+C` exits with code `130`.
- Other exceptions are logged and then printed as `error: ...` on `stderr`.

Input parsing rules:

- Required parameters become positional arguments.
- Parameters with defaults become `--kebab-case` options.
- Boolean options become `--flag` or `--no-flag`.
- `Ref` and `Uri` arguments are parsed from strings.
- `list[...]` and `dict[...]` arguments are parsed from JSON text.
- Variadic `*args: T` parameters become repeated positional arguments parsed as `T`, using `nargs="*"`.
- Parameters annotated as exactly `Any` are read from a file path argument, or from `stdin` when the path is omitted.

That matters for commands such as `dml admin cache invalidate`, which accepts repeated positional cache keys rather than one JSON list argument.

## Top-level commands

Generated directly from public `Dml` methods:

- `dml init`
- `dml status`
- `dml branch`
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
dml status
dml branch
dml log HEAD --limit 5
dml show HEAD
dml diff HEAD~1 HEAD
```

## `config` namespace

Generated from `Dml.config`:

- `dml config get KEY [--scope local|global]`
- `dml config set KEY VALUE [--scope local|global]`
- `dml config show [--contrib]`

Examples:

```bash
dml config get remote.root
dml config set remote.root s3://my-bucket/demo
dml config show
```

## `dag` namespace

Generated from `Dml.dag`:

- `dml dag get VALUE [--revision REV]`
- `dml dag describe-node NODE [--dag DAG] [--revision REV]`
- `dml dag get-node NODE [--dag DAG] [--revision REV]`
- `dml dag unroll-node NODE [--dag DAG] [--revision REV]`
- `dml dag checkout REVISION DAG_NAME [--branch BRANCH] [--target-name NAME] [--replace] [--user USER]`
- `dml dag delete NAME [--branch BRANCH] [--user USER]`

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
- `delete`
- `cancel`

This is the low-level mutable staging surface behind `daggerml.api.new()` and `Dag`.

## `admin` namespace

Generated from `Dml.admin` and nested namespaces:

- `dml admin cache invalidate`
- `dml admin remote list|gc`
- `dml admin gc [--dry-run]`

Examples:

```bash
dml runtime list
dml admin cache invalidate cache-key-1 cache-key-2
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
