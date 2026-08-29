---
name: daggerml-repository
description: Set up and manage DaggerML projects, history, remotes, dependencies, cache, and garbage collection.
---

# DaggerML Repository Management

Use the CLI for repository operations. Never edit managed `.dml/` files, refs,
configuration, or database state directly.

## Initialize Or Clone A Project

Run `dml init` inside an existing project directory. DaggerML does not search
parent directories for a project; run commands from that directory or pass
`--project-home`. A new repository begins on an unborn branch, and its first DAG
commit creates history.

Use `dml clone` for an existing remote project. The destination must not already
be initialized. Cloning a branch attaches `HEAD` and configures its upstream;
cloning a tag or exact commit creates a detached checkout.

## Configure The Project

`remote.root` is the project's synchronization, execution, cache, and artifact
endpoint. Configuration precedence is defaults, global, project, environment,
then explicit command or Python overrides.

```bash
mkdir research && cd research
dml init
dml config set remote.root s3://bucket/research
dml config show
dml status
```

## Inspect Repository State

Use `status` before and after mutations. Use `log` for history, `show` for a
revision's DAGs, `diff` for changes between revisions, and `rev-parse` to resolve
`HEAD`, `HEAD~N`, branch names, `@tag`, or exact commit hashes.

Branches and tags point to commit tips. Before `checkout`, `merge`, `rebase`,
`revert`, branch movement, or ref deletion, confirm the current branch and
revision. A non-local checkout detaches `HEAD`; create or attach a branch before
recording new history.

## Synchronize A Remote

`fetch` updates one local remote-tracking revision. `pull` fetches and integrates
the current branch's upstream, fast-forward-only by default. `push` publishes the
attached branch. For revision-reading commands, `--remote` selects fetched
tracking state rather than querying the live endpoint.

Shallow fetches retain a complete selected snapshot but omit older ancestry.
Before an operation that must prove ancestry, fetch a greater `--depth` or use
`--unshallow`.

## Use Dependencies

`dml dep add NAME ROOT` configures an import-only project. Fetch its branch or
tag with `dml fetch --dep NAME ...` before loading its DAGs. Dependencies are
read-only: never push to them or target them with garbage collection.

## Inspect And Invalidate Cache State

Cache keys identify computations; invalidation targets executions. `cache describe`
returns the pointer's exact `execution` ref, optional reusable `dag`, and
`lifecycle`, or null when no pointer exists. Inspect the execution record before
mutation, then invalidate only the exact `index:` or `frozenindex:` ref. Never
pass a cache key, bare ID, or guessed ref. Invalidation affects other users of
the same `remote.root`.

```bash
dml cache describe CACHE_KEY
dml runtime read-execution-record index:EXACT_EXECUTION_ID
dml cache invalidate index:EXACT_EXECUTION_ID
dml cache describe CACHE_KEY
```

## Garbage Collect

`dml gc` collects unreachable local objects; `dml gc --remote` collects the
configured remote. There is no dry run or dependency target. Preserve needed
refs first, and run GC only when synchronization and endpoint execution are
idle. Never run it concurrently with fetch, pull, or push.
