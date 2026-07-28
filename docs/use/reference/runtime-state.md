# Runtime state reference

Use these CLI commands for user-visible runtime operations:

```bash
dml runtime list
dml runtime describe INDEX_REF
dml runtime describe-graph INDEX_REF
dml runtime read-execution-record INDEX_REF
dml runtime cancel INDEX_REF --mode full
```

`list` returns open runtimes in reverse creation order. `describe` reports the mutable graph and parent state. `describe-graph` reports reachable execution lineage; add `--visual` for a rendered view. `cancel` accepts `full` (the default) or `drive` mode.

Lower-level `runtime create`, `put-literal`, `put-import`, `start-fn`, and `commit` exist for direct graph manipulation. Prefer `dml.new()`, `Dag`, and funks for research authoring.
