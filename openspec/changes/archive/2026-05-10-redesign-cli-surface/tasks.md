## 1. Restructure the public CLI surface

- [x] 1.1 Replace the top-level parser surface with the locked porcelain commands (`status`, `show`, `log`, `diff`, `checkout`, `branch`, `fetch`, `pull`, `push`, `merge`, `revert`, `dag`, `admin`, `config`).
- [x] 1.2 Remove legacy public command groups and update help text/examples to reflect the new breaking CLI grammar.
- [x] 1.3 Update `dml config` so `config show [--contrib]` becomes the JSON config-status entrypoint.

## 2. Add repository inspection workflows

- [x] 2.1 Add domain entrypoints for repository `status`, `show`, `log`, and `diff` so CLI handlers remain thin.
- [x] 2.2 Implement `dml show` payload generation with top-level `revision`, `commit`, `dags`, and `change` fields.
- [x] 2.3 Implement DAG-map diff computation for commit-to-base and revision-to-revision comparisons.
- [x] 2.4 Add branch listing support for both local branches and remote-tracking branches used by `dml branch` and `dml branch --remote`.

## 3. Redesign DAG inspection commands

- [x] 3.1 Replace current DAG CLI commands with `dag list`, `dag get`, `dag checkout`, and `dag delete`.
- [x] 3.2 Add revision-scoped DAG lookup by name and exact DAG lookup by `dag:<id>`, including rejection of `--revision` with explicit DAG refs.
- [x] 3.3 Expand DAG inspection payloads so `dml dag get` returns the full DAG payload including node data.

## 4. Implement admin workflows

- [x] 4.1 Add `dml admin index list|get|delete` and return commit metadata in both list and get responses.
- [x] 4.2 Add `dml admin cache invalidate <cache-key> [more-keys...]` using exact cache-key inputs only.
- [x] 4.3 Add overloaded `dml admin remote list [--owner OWNER]` and `dml admin remote list dml://<owner>/<project>` discovery workflows.
- [x] 4.4 Add `dml admin remote gc` as the unified remote maintenance command and `dml admin gc [--dry-run]` for local GC.

## 5. Verify contracts and documentation

- [x] 5.1 Update CLI contract tests to cover the new command grammar and JSON payloads, including admin index commit-info responses.
- [x] 5.2 Update repository/admin docs to match the new CLI surface and command semantics.
- [x] 5.3 Run the relevant CLI and internal contract test suites and resolve any failures.
