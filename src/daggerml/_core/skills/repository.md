---
name: daggerml-repository
description: Manage DaggerML history, references, remotes, dependencies, and garbage collection.
---

# DaggerML Repository Management

Inspect `status`, `log`, `show`, and `diff` before changing history. Branches
and tags name commit tips; `checkout`, `merge`, `rebase`, and `revert` change
the selected history, so verify the current branch and revision first. Do not
modify managed `.dml/` files, refs, or database state by hand.

`remote.root` is the project synchronization and execution-cache endpoint.
Use `fetch` to update one tracking revision, `pull` to integrate an upstream,
and `push` to publish the attached branch. Dependencies configured with
`dep add` are import-only endpoints; fetch their revisions with `--dep` before
loading their DAGs. Do not treat a dependency as a destination for project
publication or GC.

```bash
dml status
dml fetch main --depth 2
dml log
```

Shallow fetches contain complete selected commit snapshots but stop older
parent history at a recorded boundary. Deepen with a larger `--depth` or use
`--unshallow` before operations that require ancestry proof. Run local `gc` or
remote `gc --remote` only when synchronization is idle; never run GC
concurrently with fetch, pull, or push. For implementation detail, inspect
installed `daggerml._core.dml`, `daggerml._core.head`, `daggerml._core.commit`,
and `daggerml._core.remote`.
