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

`remote.root` is the project's sole S3 synchronization, cache-coordination, and remote-execution endpoint. Branches record one upstream branch name; `pull` and `push` use that branch at `remote.root`. `fetch [BRANCH|@TAG]` refreshes exactly one local tracking ref under `.dml/refs/remote/`. Import-only external projects are configured with `dml dep add NAME ROOT` and fetched with `dml fetch --dep NAME [BRANCH|@TAG]`.

```bash
dml config set remote.root s3://bucket/research
dml push
```

See [share and reuse research](../guides/share-reuse.md).
