## 1. Persisted Tree Tag Model

- [x] 1.1 Add required `tags: dict[str, list[str]]` to `Tree` and validate string names, lists of string tags, and `set(tags) <= set(dags)` without a default or legacy-payload fallback.
- [x] 1.2 Update every new-tree construction path and affected core fixtures to pass `tags={}`, including database initialization and runtime index creation.
- [x] 1.3 Add core contract coverage for valid tag maps, invalid tag shapes and unknown DAG names, and failure to load a persisted tree payload without `tags`.

## 2. Tree History Semantics

- [x] 2.1 Update private tree diff and patch operations so DAG refs and tag lists are transformed together, preserving tag-only changes during merge, revert, and rebase while treating differing changes at the same name as conflicts.
- [x] 2.2 Update DAG checkout, replacement, and deletion paths so a replacement/checkout is untagged and deletion removes the associated tag entry.
- [x] 2.3 Add contract coverage that merge, revert, and rebase preserve tags, that conflicting tag edits fail at the DAG name, and that deletion and replacement clear stale tags.

## 3. Tag Inspection And Mutation API

- [x] 3.1 Add `tags: dict[str, list[str]]` to commit description, `Dml.show()`, and `Dml.log()` payloads while retaining the existing DAG mapping and leaving the public DAG diff payload unchanged.
- [x] 3.2 Add `Dml.dag.add_tag(dag: str, tag: str) -> Ref` and `Dml.dag.remove_tag(dag: str, tag: str) -> Ref`, using the attached-HEAD lock and successor commits for state changes; make duplicate adds and absent removes idempotent no-ops that return the current commit.
- [x] 3.3 Add API contract coverage for add/remove behavior, historical inspection, idempotent operations, absent DAG errors, and detached-HEAD errors.

## 4. Documentation And Verification

- [x] 4.1 Update tree storage and history documentation to define tags as opaque per-tree-entry labels, note the `research.v0` convention example, and explicitly state the v0 storage break.
- [x] 4.2 Run `uv run --dev --all-extras ruff check --fix .`, `uv run --dev --all-extras pytest -m "not slow" .`, and `uv run --dev --all-extras pytest .`; resolve failures caused by the change.
