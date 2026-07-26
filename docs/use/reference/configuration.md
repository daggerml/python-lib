# Configuration reference

Configuration precedence is defaults, global config, project config, environment variables, then explicit CLI or Python overrides. Project state is stored under `.dml/`; project configuration is `.dml/config.json`.

Important keys:

- `default.branch_name` (defaults to `main`)
- `remote.root` (`s3://bucket` or `s3://bucket/prefix`)
- `remote.project` (`dml://owner/project`, without a branch or tag)
- `remote.fetch_workers`
- `user`

Set project values through the CLI:

```bash
dml config set remote.root s3://bucket/research
dml config set remote.project dml://alice/research
dml config show
```

Environment variables use the `DML_` prefix, including `DML_PROJECT_HOME`, `DML_REMOTE_ROOT`, `DML_REMOTE_PROJECT`, `DML_USER`, and `DML_CONFIG_HOME`. `remote.root` enables remote-backed execution and storage; `remote.project` is additionally needed for `push`, `pull`, and configured project sync.
