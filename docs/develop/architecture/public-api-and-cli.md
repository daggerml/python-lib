# Public API and CLI

## Python API

`daggerml.api` is the primary Python authoring layer. `dml.new()` creates a
mutable `dml.Dag`; `dml.load()` opens a committed named DAG; and `dml.Dag` and
`dml.Node` wrappers stage Python values, call functions, resolve results, and
delegate all repository operations to `dml.Dml`.

`dml.Dml` is the public runtime-orchestration object in `_core/dml.py`. Its
namespaces group operations such as `runtime`, `dag`, `commit`, `config`, and
`admin`. The public wrappers should not duplicate history, storage, cache, or
remote rules: those belong in the core operation modules.

`codecs.py` converts supported Python values to and from the typed datum graph.
The contrib package supplies optional codecs and integrations through package
entry points.

## CLI

The `dml` executable is defined by the `dml` project script and implemented in
`_cli.py`. `MethodCLI` derives command namespaces and arguments from the public
methods and annotated namespace attributes of `dml.Dml`; command names are
kebab-case while Python parameter names remain snake_case where positional.

When changing a public `dml.Dml` method or namespace, check both direct Python use
and its generated CLI representation. Keep argument annotations and docstrings
accurate because they supply CLI parsing and help text.
