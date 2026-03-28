# Task 08 - Restrict publication to direct DAGs

## Goal

Change publication semantics so commit and DAG publication inspect only direct DAG references at each layer, and recurse only through missing DAG refs.

## Current code anchors

- Current publication uses the full recursively dumped local closure to derive DAG ids: `src/daggerml/_internal/ops/remote.py:694`, `src/daggerml/_internal/ops/remote.py:703`, `src/daggerml/_internal/ops/remote.py:995`.
- Current DAG recursion in publication uses all DAG ids found in the dumped local manifest closure: `src/daggerml/_internal/ops/remote.py:734`.
- Commit trees hold the direct named DAG set at `Tree.dags`: `src/daggerml/_internal/types.py:742`.
- DAG nodes can directly reference other DAGs via node fields such as `ImportNode.dag` and `FnNode.dag`: `src/daggerml/_internal/types.py:555`, `src/daggerml/_internal/types.py:591`.
- Planning constraint: this task changes publication semantics in code, not stable external runtime payload names.

## Implement

- Add a helper that computes direct DAG ids for a publication root:
  - for `commit` roots: exactly the DAG ids from that commit's `Tree.dags` values
  - for `dag` roots: exactly the direct child DAG ids referenced by that DAG's own nodes
  - for other roots: direct DAG ids reached from the root-owned object graph without traversing into child DAG roots
- Update manifest construction so `closure["dag"]` contains only those direct DAG ids.
- Update ref `targets["dag"]` construction so it also contains only those direct DAG ids.
- Update `_ensure_dag_ref(...)` so it:
  - returns immediately if the DAG ref already exists remotely
  - inspects and recurses only into the current DAG's direct child DAG ids when the current DAG ref is missing
- Update `put_ref_manifest(...)`, `push(...)`, and cache publication to use the direct-DAG helper instead of deriving DAG ids from the full dumped closure.

## Inputs and outputs

- `RemoteOps._direct_dag_ids(txn, root_ref: Ref) -> list[str]`
  - input: readonly transaction and publication root ref
  - output: sorted unique direct DAG ids for that root
  - errors: raises `ValueError` on unsupported root namespace or invalid DAG refs encountered in the direct graph
- `RemoteOps._ensure_dag_ref(dag_ref: Ref) -> bool`
  - unchanged public signature
  - changed behavior: recurse only through missing direct child DAGs

## IO

- Reads local commit/tree/DAG objects to discover direct DAG references.
- Reads `refs/dags/<dag_id>.json` for direct DAG existence checks.
- Writes only the DAG manifests/refs that are actually needed by missing direct DAG refs and their missing descendants.

## Expected behavior to test

- Commit publication with direct DAG `A` and transitive DAG `B`:
  - if `A` already has a DAG ref remotely, push must not inspect or ensure `B`
  - tag ref `targets["dag"]` must list only `A`
  - top manifest `closure["dag"]` must list only `A`
- DAG publication with direct child DAG `B`:
  - if `B` already has a DAG ref remotely, publishing the parent DAG must not inspect descendants of `B`
  - if `B` is missing, publication must recurse into `B`
- Cache publication for a DAG root must use only that DAG's direct child DAG ids in `targets`.
- Existing pull/load behavior must continue to recurse by manifest `closure["dag"]` entries, which are now direct-only.

## Done when

- Publication no longer derives DAG publication work from the transitive dumped closure.
- Each manifest/ref layer records and ensures only its direct DAG references.
