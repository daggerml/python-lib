## Why

Fetching a project or dependency currently materializes every historical commit and every object reachable from it, even when a caller only needs the selected commit's current DAG snapshot. Large histories therefore impose unnecessary network, storage, and startup costs for clones and dependency consumption.

## What Changes

- Add positive integer commit-history depth selection to clone, fetch, dependency fetch, and pull workflows.
- Always materialize the complete tree, DAG, node, and data closure for each included commit while limiting traversal only across commit-parent edges.
- Record intentionally unavailable commit parents as repository-local shallow-history metadata without changing immutable DML objects or remote CAS/ref formats.
- Support explicit unshallowing and incremental fetch/pull of repositories that already contain shallow history.
- Make revision traversal, history inspection, ancestry checks, merges, rebases, reverts, status, publication, and local garbage collection distinguish shallow boundaries from repository corruption.
- Preserve safe defaults: operations that require unavailable ancestry fail with deepening guidance instead of inferring unrelated history or a non-ancestor relationship.
- Keep `dep add` configuration-only; callers request dependency depth through `fetch --dep`.

## Capabilities

### New Capabilities

- `shallow-history-materialization`: Defines commit-depth semantics, complete snapshot guarantees, local missing-parent metadata, deepening, unshallowing, and availability-aware traversal.

### Modified Capabilities

- `clone-bootstrap-workflow`: Allows clone to request bounded commit history while still creating a usable selected checkout.
- `dependency-dag-imports`: Allows project and dependency fetches to request depth or unshallow existing history without weakening imported DAG completeness.
- `remote-object-refs`: Changes project commit materialization and publication to understand intentionally omitted commit-parent closures while preserving complete remote refs.
- `remote-project-refs`: Defines depth-aware tracking-ref fetch and shallow pull behavior.
- `git-like-commit-ops`: Defines safe inspection, status, ancestry, mutation, and publication behavior when local commit ancestry is incomplete.

## Impact

- Public `Dml.clone`, `Dml.fetch`, and `Dml.pull` signatures and their generated CLI commands gain shallow-history options.
- Core changes affect `dml.py`, `remote.py`, `commit.py`, `head.py`, local GC integration, and the database reachability boundary.
- Repository-local metadata under `.dml/` gains a shallow-history registry; immutable `Commit`, `Tree`, DAG, node, remote ref, and remote CAS schemas remain unchanged.
- History and status payloads may need to expose truncation or unavailable ancestry explicitly.
- Contract, integration, CLI-generation, remote-roundtrip, and GC tests require shallow linear-history, merge-history, incremental pull, deepening, corruption, and publication coverage.
