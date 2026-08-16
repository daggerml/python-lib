# Runtime state reference

Use these CLI commands for user-visible runtime operations:

```bash
dml runtime list
dml runtime describe index:<execution-id>
dml runtime describe-graph index:<execution-id>
dml runtime read-execution-record index:<execution-id>
dml runtime read-launch-state index:<execution-id>
dml runtime cancel index:<execution-id> --mode full
```

`list` returns open runtimes in reverse creation order. Pass the complete ref it returns, including the `index:` or `frozenindex:` namespace, to runtime inspection and cancellation commands. The generated CLI converts that text to a `Ref` before invoking `Dml`; bare execution IDs are not accepted by these commands.

`describe` reports the mutable graph and parent state. `describe-graph` reports reachable execution lineage; add `--visual` for a rendered view. Rendered terminal views require `pip install "daggerml[terminal]"`. `cancel` accepts `full` (the default) or `drive` mode.

Lower-level `runtime create`, `put-literal`, `put-import`, `start-fn`, and `commit` exist for direct graph manipulation. Execution-aware creation uses `--execution index:<execution-id>`. Prefer `dml.new()`, `Dag`, and funks for research authoring.
