# History and remotes

Each commit records a tree of named DAG snapshots. A tree can also attach opaque
tags to a named DAG entry, preserving its classification in commit history. Use
`Dml.dag.add_tag(name, tag)` and `Dml.dag.remove_tag(name, tag)` on an attached
branch to update those labels. For example, `research.v0` can identify a
project-defined research DAG schema; DaggerML does not interpret or validate
that convention. Branches and tags point at commits; `HEAD` selects the current
checkout. Use `dml log`, `dml show`, `dml diff`, branches, and tags to inspect or
organize research history.

Tree tags are part of the current v0 persisted tree format. Repositories whose
stored trees predate required tag data are not compatible with this format.

An S3 remote has two roles. `remote.root` provides remote storage, cache coordination, and remote-backed execution. Named project remotes identify projects for history synchronization. Branches record one upstream such as `origin/main`; `pull` and `push` use that upstream, while `fetch [REMOTE]` refreshes local tracking refs for one remote.

```bash
dml config set remote.root s3://bucket/research
dml remote add origin dml://alice/research
dml push
```

See [share and reuse research](../guides/share-reuse.md).
