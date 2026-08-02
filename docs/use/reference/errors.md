# Error reference

`daggerml.Error` stores `message`, `origin`, `type`, and `stack`; it records a failed DAG terminal state. `DmlRepoError`, a subclass of `Error`, covers most project, configuration, runtime, and execution failures. See [Errors](../concepts/errors.md) for error capture, function failures, inspection, and provenance.

| Message or condition | Action |
| --- | --- |
| `DAG not found: ...` | Run `dml show` and confirm the DAG name and checkout. |
| `remote.root is required` | Configure an S3 `remote.root`. |
| `remote.project is required for project sync` | Configure `remote.project` before project sync. |
| Detached checkout cannot commit | Attach `HEAD` with `dml checkout BRANCH`. |
| Function timeout | Inspect the runtime graph and execution boundary before retrying. |
| Codec staging failure | Convert the value to a supported type or provide a codec. |
| Local database map is full at its configured maximum | Increase `default.db_map_size_max` if the filesystem and address space permit it, or garbage-collect unreachable local history. |

CLI parse errors exit 2. `Ctrl+C` exits 130. Other CLI failures are printed on standard error. Use `Dag` as a context manager when you want an uncaught Python exception committed as an inspectable `Error` result.
