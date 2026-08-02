# Configuration reference

Configuration precedence is defaults, global config, project config, environment variables, then explicit CLI or Python overrides. Project state is stored under `.dml/`; project configuration is `.dml/config.json`.

Important keys:

- `default.branch_name` (defaults to `main`)
- `default.db_map_size_headroom` (defaults to 50 MiB; fixed growth increment for local LMDB storage, except for the final increment capped by the maximum)
- `default.db_map_size_max` (defaults to 10 GiB; maximum local LMDB map size)
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

Local LMDB writes grow the map automatically when necessary. Growth retries stop at `default.db_map_size_max`; raise that setting when a capacity error reports that the map has reached its configured maximum.
