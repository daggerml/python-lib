---
name: daggerml-repository
description: Manage DaggerML history, references, remotes, dependencies, and garbage collection.
---

# DaggerML Repository Management

Before changing history, inspect `status`, `log`, `show`, and `diff`. Branches
and tags name commit tips; confirm the current branch and revision before
`checkout`, `merge`, `rebase`, or `revert`, then inspect the result. Do not
modify managed `.dml/` files, refs, or database state by hand.

`remote.root` is the project synchronization and execution-cache endpoint. Use
`fetch` to update one tracking revision, inspect it locally, then `pull` to
integrate an upstream or `push` to publish the attached branch. Dependencies
configured with `dep add` are import-only endpoints: fetch their revisions with
`--dep` before loading DAGs. Never publish to or run GC against a dependency.

```bash
dml status
dml fetch main --depth 2
dml log
```

Shallow fetches contain complete selected snapshots but stop parent history at a
recorded boundary. Before operations requiring ancestry proof, deepen with a
larger `--depth` or use `--unshallow`. Run local `gc` or remote `gc --remote`
only when synchronization is idle; never run GC concurrently with fetch, pull, or push.
Inspect `status` after synchronization or GC. For implementation detail, inspect
installed `daggerml._core.dml`, `daggerml._core.head`, `daggerml._core.commit`,
and `daggerml._core.remote`.
