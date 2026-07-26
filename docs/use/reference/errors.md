# Error reference

`daggerml.Error` stores `message`, `origin`, `type`, and `stack`; it can be a committed DAG result. `DmlRepoError`, a subclass of `Error`, covers most project, configuration, runtime, and execution failures.

| Message or condition | Action |
| --- | --- |
| `DAG not found: ...` | Run `dml show` and confirm the DAG name and checkout. |
| `remote.root is required` | Configure an S3 `remote.root`. |
| `remote.project is required for project sync` | Configure `remote.project` before project sync. |
| Detached checkout cannot commit | Attach `HEAD` with `dml checkout BRANCH`. |
| Function timeout | Inspect the runtime graph and execution boundary before retrying. |
| Codec staging failure | Convert the value to a supported type or provide a codec. |

CLI parse errors exit 2. `Ctrl+C` exits 130. Other CLI failures are printed on standard error. Use `Dag` as a context manager when you want an uncaught Python exception committed as an inspectable `Error` result.
