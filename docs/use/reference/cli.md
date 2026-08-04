# CLI reference

Run `dml --help` for the generated command surface. Global project options include `--project-home`, `--remote-root`, `--remote-project`, `--user`, and `--config-home`.

| Purpose | Commands |
| --- | --- |
| Project | `init`, `clone`, `status`, `config show`, `config set` |
| History | `log`, `show`, `diff`, `checkout`, `merge`, `rebase`, `revert`, `rev-parse` |
| Refs | `branch list|create|move|rename|delete|set-upstream`, `tag list|create|delete` |
| Sync | `remote add|list|delete`, `fetch [REMOTE]`, `pull`, `push` |
| Runtimes | `runtime list|describe|freeze|unfreeze|describe-graph|cancel` |
| Remote cache | `admin remote get-cache`, `admin remote invalidate-cache` |
| Cleanup | `admin gc`, `admin remote gc` |
| Agent guidance | `admin agent-skill` |
| Error inspection | `dag get-node NODE_REF`, `dag get-error ERROR_REF` |

Commands print successful scalar values or compact JSON to standard output. Parse errors exit with code 2; other errors print `error: ...` to standard error. The CLI is for administration, not for passing live callables. Current authoring tooling creates DAGs and funks in Python.

Export the bundled, portable coding-agent skill with `dml admin agent-skill > SKILL.md`.
