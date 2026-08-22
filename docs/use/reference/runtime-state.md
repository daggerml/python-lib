# Runtime state reference

Use these CLI commands for user-visible runtime operations:

```bash
dml runtime list
dml runtime describe index:<execution-id>
dml runtime describe-graph index:<execution-id>
dml runtime read-execution-record index:<execution-id>
dml runtime cancel index:<execution-id> --max-retries 3
```

`list` returns open runtimes in reverse creation order. Pass the complete ref it returns, including the `index:` or `frozenindex:` namespace, to runtime inspection and cancellation commands. The generated CLI converts that text to a `Ref` before invoking `Dml`; bare execution IDs are not accepted by these commands.

`describe` reports the mutable graph and parent state. `read-execution-record`
returns exact `metadata`, `state`, and `driver` sections. Metadata contains
immutable identity and argv fields; state contains lifecycle, result, lineage,
cancelation, and invalidation; driver contains the lock, adapter continuation,
shared retry delay, and cleanup outcome. Cleanup may remain pending or fail
without making an otherwise reusable result unavailable. `describe-graph`
reports reachable execution lineage; add `--visual` for a rendered view.
Rendered terminal views require `pip install "daggerml[terminal]"`. `cancel`
runs or resumes both cancellation phases and retries unsuccessful adapters up to
`--max-retries` times after the initial parallel attempt.

Lower-level `runtime create`, `put-literal`, `put-import`, `start-fn`, and `commit` exist for direct graph manipulation. Execution-aware creation uses `--execution index:<execution-id>`. Prefer `dml.new()`, `Dag`, and funks for research authoring.
