# History and remotes

Each commit records a tree of named DAG snapshots. Branches and tags point at commits; `HEAD` selects the current checkout. Use `dml log`, `dml show`, `dml diff`, branches, and tags to inspect or organize research history.

An S3 remote has two roles. `remote.root` provides remote storage, cache coordination, and remote-backed execution. `remote.project` identifies a project for `fetch`, `pull`, and `push`. Configure them with `dml config set`.

```bash
dml config set remote.root s3://bucket/research
dml config set remote.project dml://alice/research
dml push
```

See [share and reuse research](../guides/share-reuse.md).
