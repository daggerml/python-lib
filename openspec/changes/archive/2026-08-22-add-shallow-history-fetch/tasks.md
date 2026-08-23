## 1. Shallow Repository State

- [x] 1.1 Add versioned atomic read/write operations for the repository-local set of intentionally absent commit refs, including strict malformed-state and namespace validation, and verify focused Head contract tests cover empty, valid, stale, and invalid metadata.
- [x] 1.2 Extend native/Python orphan traversal to accept only declared absent commit leaves while continuing to reject every undeclared missing object, and verify focused DB and local-GC contract tests pass.

## 2. Commit-Aware Remote Materialization

- [x] 2.1 Separate project commit ancestry traversal from complete generic object traversal so each included commit materializes its full snapshot closure, and verify remote-roundtrip tests cover depth one, linear depth, merge-parent generations, and missing snapshot failure.
- [x] 2.2 Implement positive-depth validation, ordinary incremental update mode, repeated depth expansion, and explicit unshallow traversal through locally present frontier commits, and verify integration tests cover boundary movement, existing deeper history preservation, and mutually exclusive options.
- [x] 2.3 Publish shallow metadata before the selected tracking pointer after replayable object writes, preserving prior tracking state on failure, and verify fault-injection integration tests cover download, local-write, metadata-write, and tracking-write failures.

## 3. Public Workflows And CLI

- [x] 3.1 Add depth support to `Dml.clone`, including branch, tag, default-branch, and exact-commit selections, and verify clone integration tests cover attached/detached state, complete snapshots, shallow parents, invalid depth, and failed bootstrap.
- [x] 3.2 Add `depth` and `unshallow` selection to project and dependency `Dml.fetch` while keeping `dep.add` configuration-only, and verify fetch/dependency integration tests cover branch, tag, default branch, deepening, and complete imported DAG consumption.
- [x] 3.3 Add optional depth to `Dml.pull` so ordinary pull connects new remote history to the existing local tip while preserving older boundaries, and verify pull integration tests cover shallow fast-forward and insufficient-depth refusal without branch advancement.
- [x] 3.4 Update generated CLI parsing/help contracts for `clone --depth`, `fetch --depth`, `fetch --unshallow`, dependency fetch, and `pull --depth`, and verify CLI constructor and command-routing tests pass.

## 4. History And Revision Safety

- [x] 4.1 Introduce availability-aware commit traversal that distinguishes proven results from shallow-unknown ancestry and merge bases, and verify focused commit contracts cover linear and merge histories on both sides of a boundary.
- [x] 4.2 Make log expose truncation and make `HEAD~N`, show, and implicit-parent diff return deepening guidance at shallow boundaries while explicit available-snapshot diff remains usable, and verify repository inspection and revision-resolution contracts pass without network access.
- [x] 4.3 Make status return unavailable ahead/behind counts and make merge, rebase, and revert fail without mutation when required ancestry is shallow-unknown while allowing relationships proven above the boundary, and verify focused history mutation and status contracts pass.

## 5. Publication And Collection

- [x] 5.1 Make local object collection terminate only at declared absent commit refs and permit non-forced branch updates only when the observed remote tip anchors the omitted history, and verify remote integration tests cover safe shallow updates and complete uploaded current snapshots.
- [x] 5.2 Reject new-ref creation, forced publication, and unknown-ancestry updates from shallow history with unshallow guidance, and verify each rejection leaves remote refs unchanged.
- [x] 5.3 Normalize stale shallow entries during local garbage collection and verify GC preserves retained shallow snapshots, removes unrelated objects and stale entries, and still reports undeclared missing refs as corruption.

## 6. Documentation And Verification

- [x] 6.1 Update CLI, history/remotes, share/reuse, architecture, and sharp-bits documentation with depth semantics, complete snapshot guarantees, deepening/unshallowing, safe-operation limits, and rollback guidance, and verify documentation links and command examples match generated help.
- [x] 6.2 Run `openspec validate add-shallow-history-fetch --strict`, focused core contract/integration suites, `uv run --dev --all-extras pytest .`, and `uv run --dev --all-extras ruff check .`, resolving all failures before marking the change complete.
