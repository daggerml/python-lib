## 1. Export Contract

- [x] 1.1 Update bundled-skill contract tests for exactly `querying`, `authoring`, `repository`, and `extensions`, including frontmatter, portability, example limits, topic-specific guidance, and the 1000-word ceiling.
- [x] 1.2 Replace the `inspection` Python/CLI export with `querying` and add the `extensions` export, with no compatibility alias.

## 2. Skill Resources

- [x] 2.1 Replace `inspection.md` with a self-contained `querying.md` covering DAG discovery, results and named nodes, projections, materialization, provenance traversal, and persisted error capture while excluding cache control.
- [x] 2.2 Revise `authoring.md` to preserve nodes and projections across authoring and nested-funk boundaries, and add an example that calls a funk with a node directly before using `.value()` for concrete worker-side computation.
- [x] 2.3 Revise `repository.md` to cover setup and configuration plus cache inspection, exact-execution-ref validation, intentional invalidation, and existing history/remote/dependency/GC safety.
- [x] 2.4 Add `extensions.md` covering adapter, executor, and codec boundaries; lifecycle and response contracts; nested forwarding; plugin registration; script isolation; and contract-first testing.
- [x] 2.5 Review all four resources for factual accuracy, independent usefulness, topic separation, and removable cruft below the 1000-word maximum.
- [x] 2.6 Prune cross-topic prerequisites, implementation-detail pointers, and repeated caveats from all four skills.

## 3. Documentation

- [x] 3.1 Update Python API and generated CLI documentation to list the four exports and migrate references from `inspection` to `querying`.
- [x] 3.2 Update any user guidance that assigns cache inspection or invalidation to the old inspection skill.

## 4. Verification

- [x] 4.1 Run focused bundled-skill and CLI contract tests.
- [x] 4.2 Run required type checking, lint-fix, and non-slow tests; confirm packaged resources export exactly as authored.
