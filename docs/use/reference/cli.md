# CLI reference

Run `dml --help` for the generated command surface. Global project options include `--project-home`, `--remote-root`, `--remote-project`, `--user`, and `--config-home`.

| Purpose | Commands |
| --- | --- |
| Project | `init`, `clone`, `status`, `config show`, `config set` |
| History | `log`, `show`, `diff`, `checkout`, `merge`, `rebase`, `revert`, `rev-parse` |
| Refs | `branch list|create|move|rename|delete|set-upstream|get-upstream`, `tag list|create|delete` |
| Sync | `dep add|list|delete`, `fetch [--dep DEP] [BRANCH|@TAG]`, `pull`, `push` |
| Runtimes | `runtime list|describe|freeze|unfreeze|read-execution-record|read-launch-state|describe-graph|cancel` |
| Remote cache | `cache get`, `cache invalidate` |
| Cleanup | `gc [--remote]` |
| Agent guidance | `admin agent-skill` |
| Error inspection | `dag get-node NODE_REF`, `dag get-error ERROR_REF` |

Commands print successful scalar values or compact JSON to standard output. Parse errors exit with code 2; other errors print `error: ...` to standard error. The CLI is for administration, not for passing live callables. Current authoring tooling creates DAGs and funks in Python.

`branch list` and `tag list` return JSON arrays whose items contain `name` and exact `commit` fields. Both commands accept `--remote` and `--dep DEP`: neither flag lists local refs, `--remote` lists `remote.root`, `--dep` lists fetched dependency refs, and both together list the dependency endpoint. Endpoint listing is read-only and does not fetch or update tracking refs. This structured array replaces the earlier array of names. Revision-consuming commands continue to reject `--remote` and `--dep` together.

`cache get KEY` resolves a cached DAG and `cache invalidate KEY...` invalidates one or more exact cache keys. `gc` collects unreachable local objects by default; `gc --remote` collects configured `remote.root` state. GC does not accept dependency or dry-run selectors. These commands replace the removed `admin remote get-cache`, `admin remote invalidate-cache`, `admin remote gc`, and `admin gc` paths without aliases.

Export the bundled, portable coding-agent skill with `dml admin agent-skill > SKILL.md`.
