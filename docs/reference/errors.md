# Errors

DaggerML surfaces a small set of important error layers to users.

## Python-visible error types

### `daggerml.Error`

`Error` is both an exception type and a stored DAG value. It carries:

- `message`
- `origin`
- `type`
- `stack`

When a DAG is used as a context manager, uncaught exceptions are converted with `Error.from_ex(...)` and committed as the DAG result.

### `DmlRepoError`

Most repository, runtime, config, revision, and execution failures surface as `DmlRepoError`, a subclass of `Error`.

Common examples from the current code:

- `No default Dml is configured`
- `DAG not found: <name>`
- `No active index`
- `Cannot set node names on a committed DAG.`
- `Current checkout is detached; attach HEAD to commit`
- `remote.project is required for project sync`
- `remote.root is required`
- `Unknown kwarg: <key>`
- `Adapter output must be JSON`
- `Remote context required for adapter invocation`

### Low-level database errors

The native database layer defines `DmlDbError` and many subclasses such as `DmlDbMapFullError` and `DmlDbEnvReopenedError`.

Those types are mostly internal. The repo layer retries some of them automatically and usually re-surfaces user-facing failures as `DmlRepoError`.

## What different surfaces do

### Python API

- `load(name, ...)` raises `DmlRepoError` for a missing DAG name.
- `Dag.call(...)` and `RunnableNode(...)` raise `TimeoutError` when the timeout expires.
- Invalid node-name key types raise `TypeError`.
- Codec staging failures are wrapped as `DmlRepoError`.
- `node.argv` raises `Error("Node has no argv", origin="dml", type="TypeError")` when the node has no argv list.

### CLI

The current CLI does not return a structured JSON error envelope.

- Parse and usage failures come from `argparse` and exit with code `2`.
- `KeyboardInterrupt` exits with code `130`.
- Other failures are logged with `logging.exception(...)` and then printed as `error: <message>` on `stderr`.
- Successful command results still go to `stdout` as JSON.

## Retry behavior

The internal transaction wrapper retries whole operations when the database raises:

- `DmlDbMapFullError`: after resizing the database
- `DmlDbEnvReopenedError`: after the environment has been reopened

That retry loop is internal; callers generally see either success or the final surfaced exception.

## Practical debugging tips

- If a sync command fails with `remote.project is required for project sync`, configure `remote.project` in `.dml/config.toml` or with `dml config set`.
- If a remote-backed flow fails with `remote.root is required`, set `remote.root` in config, via environment, or through `Dml(...)` / CLI flags.
- If a function call fails with `Unknown kwarg: ...`, check the runnable's accepted keyword parameters.
- If an adapter run fails with `Adapter output must be JSON`, inspect the adapter process output before it reaches DaggerML.

## Related pages

- [Python API](python-api.md)
- [CLI](cli.md)
- [Configuration](configuration.md)
