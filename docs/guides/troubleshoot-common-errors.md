# Troubleshoot common errors

These are the most common workflow errors surfaced by the current CLI and Python API.

## `remote.root is required`

You will hit this when you ask DaggerML to bootstrap or recover a remote project without telling it where the remote data lives.

Example:

```bash
dml init --project-home ./demo-repo --remote-project dml://alice/demo
```

Fix it by adding `--remote-root s3://bucket/prefix` or setting `DML_REMOTE_ROOT` first.

## `remote.project is required for project sync`

You will hit this when you try `fetch`, `pull`, or `push` in a repo that has a remote root but no configured project URI.

Fix it by setting `remote.project`:

```bash
dml --project-home ./demo-repo config set remote.project dml://alice/demo
```

## `DAG not found: <name>`

You will hit this when `load("name")` or `dml dag get name` points at a DAG that is not present in the selected revision.

Start by checking what exists:

```bash
dml --project-home ./demo-repo show
```

If the DAG exists on another revision, pass that revision explicitly.

## `Current checkout is detached; attach HEAD to commit`

You will hit this when you try to commit through the Python API while `HEAD` is detached.

Fix it by reattaching `HEAD` to a branch before creating the DAG commit:

```bash
dml --project-home ./demo-repo checkout main
```

## `Unknown kwarg: <key>`

You will hit this when `dag.call(...)` passes a keyword argument that the runnable does not accept.

Example:

```python
result = dag.call(fn, 1, 2, 3, y=100)
```

Fix it by matching the runnable's declared keyword arguments.

## `S3Store requires configured remote.root`

You will hit this when you call `S3Store()` without a configured S3 remote root.

Fix it in one of two ways:

- configure `remote.root` for the repo and then call `S3Store()`
- or use `S3Store.from_remote_root("s3://bucket/prefix")`

## When the CLI fails early

The CLI prints normal command results as JSON. Failures are shown as errors on stderr instead. If a command shape looks right but still fails, check the built-in help for the generated option names:

```bash
dml diff --help
dml pull --help
dml dag get --help
```

## Related docs

- [Reference home](../reference/README.md)
- [CLI](../reference/cli.md)
- [Python API](../reference/python-api.md)
- [Errors](../reference/errors.md)
