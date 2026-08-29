# CLI reference

Run `dml --help` for the generated command surface. Global project options include `--project-home`, `--remote-root`, `--user`, and `--config-home`.

| Purpose | Commands |
| --- | --- |
| Project | `init`, `clone [--depth N]`, `status`, `config show`, `config set` |
| History | `log`, `show`, `diff`, `checkout`, `merge`, `rebase`, `revert`, `rev-parse` |
| Refs | `branch list|create|move|rename|delete|set-upstream|get-upstream`, `tag list|create|delete` |
| Sync | `dep add|list|delete`, `fetch [--dep DEP] [--depth N|--unshallow] [BRANCH|@TAG]`, `pull [--depth N]`, `push` |
| Runtimes | `runtime list|describe|freeze|unfreeze|read-execution-record|describe-graph|cancel` |
| Remote cache | `cache get`, `cache describe`, `cache invalidate` |
| Cleanup | `gc [--remote]` |
| Agent guidance | `skills querying|authoring|repository|extensions` |
| Error inspection | `dag get-node NODE_REF`, `dag get-error ERROR_REF` |

Commands print successful scalar values or compact JSON to standard output. Parse errors exit with code 2; other errors print `error: ...` to standard error. The CLI is for administration, not for passing live callables. Current authoring tooling creates DAGs and funks in Python.

`branch list` and `tag list` return JSON arrays whose items contain `name` and exact `commit` fields. Both commands accept `--remote` and `--dep DEP`: neither flag lists local refs, `--remote` lists `remote.root`, `--dep` lists fetched dependency refs, and both together list the dependency endpoint. Endpoint listing is read-only and does not fetch or update tracking refs. This structured array replaces the earlier array of names. Revision-consuming commands continue to reject `--remote` and `--dep` together.

`clone --depth N`, `fetch --depth N`, and `pull --depth N` bound commit
ancestry only. Depth one includes the selected tip. Every included commit still
has its complete tree and DAG object closure, and merge commits include every
parent at the next generation. `fetch --unshallow` downloads all ancestry for
the selected branch or tag; it cannot be combined with `--depth`. Repeating a
fetch with greater depth adds history and never removes objects already present.
`dep add` remains configuration-only, so use `fetch --dep NAME --depth N` to
materialize a shallow dependency revision.

`cache get CACHE_KEY` resolves a cached DAG. `cache describe CACHE_KEY` emits JSON
for the cache-pointer snapshot with `execution`, `dag`, and `lifecycle`; `dag`
is null unless the selected execution has an unmarked reusable terminal result.
Use the returned execution ref with `cache invalidate EXECUTION_REF
[MORE_EXECUTION_REF ...]`, for example `dml cache invalidate index:e1`.
Invalidation accepts `index:` and `frozenindex:` refs, not cache keys or bare
IDs. `gc` collects unreachable local objects by default; `gc --remote` collects
configured `remote.root` state. GC does not accept dependency or dry-run
selectors.

Export one bundled, portable coding-agent skill with `dml skills querying > SKILL.md`, `dml skills authoring > SKILL.md`, `dml skills repository > SKILL.md`, or `dml skills extensions > SKILL.md`. Use `querying` for data extraction, DAG traversal, provenance, and persisted errors; use `repository` for cache inspection and invalidation.

## Local dashboard

Install the dashboard extra and launch it from a DaggerML project:

```bash
pip install "daggerml[dashboard]"
dml-dashboard
```

The standalone dashboard launcher accepts `--config-home` (with
`--config-dir` as an alias), `--host`, `--port`, and `--no-open`. It discovers
projects from that configuration directory and does not accept a project-path
argument. It defaults to `127.0.0.1:8765` and opens a browser. A
non-loopback host is refused unless `--allow-remote` is supplied; that mode
requires the ephemeral bearer token printed at startup. An uninitialized
configuration opens a diagnostic empty state.
