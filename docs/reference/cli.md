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

That last rule matters for commands such as `dml admin cache invalidate`, which currently expects one JSON list argument rather than repeated positional cache keys.

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

- `dml dag list [--revision REV]`
- `dml dag describe VALUE [--revision REV]`
- `dml dag get VALUE [--revision REV]`
- `dml dag describe-node NODE_SELECTOR [--dag-selector DAG] [--revision REV]`
- `dml dag get-node NODE_SELECTOR [--dag-selector DAG] [--revision REV]`
- `dml dag unroll-node NODE_SELECTOR [--dag-selector DAG] [--revision REV]`
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
- `cancel`

This is the low-level mutable staging surface behind `daggerml.api.new()` and `Dag`.

## `admin` namespace

Generated from `Dml.admin` and nested namespaces:

- `dml admin index list|get|delete`
- `dml admin cache invalidate`
- `dml admin remote list|gc`
- `dml admin gc [--dry-run]`

Examples:

```bash
dml admin index list
dml admin cache invalidate '["cache-key-1","cache-key-2"]'
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
