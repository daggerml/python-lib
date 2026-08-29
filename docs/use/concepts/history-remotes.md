# History and remotes

Each commit records a tree of named DAG snapshots. DAGs themselves carry opaque
tags, so their classification survives result publication, imports, cache reuse,
and being named in a different tree. For example, `research.v0` can identify a
project-defined research DAG schema; DaggerML does not interpret that convention.
Branches and tags point at commits; `HEAD` selects the current checkout. Use
`dml log`, `dml show`, `dml diff`, branches, and tags to inspect or organize
research history; inspect an individual DAG to read its tags.

`remote.root` is the project's sole S3 synchronization, cache-coordination, and remote-execution endpoint. Branches record one upstream branch name; `pull` and `push` use that branch at `remote.root`. `fetch [BRANCH|@TAG]` refreshes exactly one local tracking ref under `.dml/refs/remote/`. Import-only external projects are configured with `dml dep add NAME ROOT` and fetched with `dml fetch --dep NAME [BRANCH|@TAG]`.

`Dml.branch.list()` and `Dml.tag.list()` return ordered `{"name": ..., "commit": Ref(...)}` items. With no selectors they inspect local refs; `remote=True` inspects `remote.root`; `dep="models"` inspects fetched dependency refs; and both selectors inspect the dependency endpoint. Endpoint inspection reads exact remote tips without fetching their commits, materializing CAS data, updating tracking refs, or initializing an empty endpoint. A remote tip can therefore be listed even when its commit is unavailable locally. Use `show()` only after the tip is locally available.

Use `Dml.branch.get_upstream(name)` to inspect any branch's configured upstream independently of the current checkout. Upstream metadata remains branch-only.

Clone and fetch can limit local commit history with `--depth N`. This never
creates a partial research snapshot: the selected commit's tree, DAGs, nodes,
data, and imports are complete, while only older commit parents can remain
unavailable. DaggerML records those parent refs as local shallow-history state,
so missing history is distinguishable from a damaged object graph.

An ordinary pull of a shallow branch downloads new commits through the existing
local tip and preserves its older boundary. Use a larger `fetch --depth N` to
deepen selected history or `fetch --unshallow` to retrieve it completely.
History operations that cannot prove ancestry stop with deepening guidance;
logs identify truncated history and status leaves unknown ahead/behind counts
unavailable rather than reporting partial values.

```bash
dml config set remote.root s3://bucket/research
dml push
```

See [share and reuse research](../guides/share-reuse.md).
