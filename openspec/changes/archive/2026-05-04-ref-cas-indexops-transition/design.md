## Context

The repository now stores branch and index refs as files under `.dml/refs/**`, but important `IndexOps` mutation paths still treat ref updates as if they were LMDB-internal pointer writes. That creates a mismatch between immutable commit creation in LMDB and mutable ref publication on the filesystem: `IndexOps` can write branch or index files while the LMDB transaction that created the target commit is still open.

The desired ownership boundary is sharper than the current code:

- `HeadOps` owns all interaction with `.dml/refs/**`.
- `IndexOps` owns commit derivation in LMDB.
- Ref publication happens only after LMDB commit success.

This is cross-cutting because `HeadOps` API shape, `IndexOps` mutation sequencing, temporary builtin scratch-index flows, and pointer-conflict retry behavior all change together.

## Goals / Non-Goals

**Goals:**
- Ensure branch and index files never move to commits that failed to commit in LMDB.
- Keep `.dml/refs/**` access encapsulated inside `HeadOps`.
- Define a single optimistic publication pattern for affected `IndexOps` mutation paths: derive commit in LMDB, close transaction, CAS through `HeadOps`, retry on conflict.
- Remove temporary index ref files from builtin and failed-execution helper flows.

**Non-Goals:**
- Introduce a journal, background replay system, or new ref persistence backend.
- Change public CLI or API semantics beyond conflict timing and retry behavior implied by the new publication order.
- Redesign unrelated branch, tag, or remote-tracking ref behavior.

## Decisions

### Decision: `HeadOps` remains the only `.dml/refs/**` boundary
All branch and index lookup, create, compare-and-swap update, listing, and deletion stays behind `HeadOps` public methods.

Rationale:
- This preserves the existing architectural rule that callers work with branch names, index ids, and commit refs rather than raw ref paths.
- It prevents `IndexOps` or helper code from reintroducing direct filesystem access while fixing the transaction-ordering bug.

Alternatives considered:
- Let `IndexOps` read/write ref files directly during retry loops. Rejected because it breaks encapsulation and duplicates stale-write handling.

### Decision: non-bootstrap `HeadOps` pointer methods are file-I/O-only and transaction-free
`get_branch_commit`, `get_index_commit`, `update_branch_commit`, `update_index_commit`, `create_index`, `delete_index`, `list_branches`, `list_indexes`, and `list_pointer_roots` operate only on filesystem refs and pointer-file compare-and-swap state. They do not accept caller-owned transactions and do not open LMDB transactions for commit validation.

Rationale:
- These methods represent file-backed ref operations, not DB mutations.
- Removing transaction participation prevents callers from assuming ref publication is atomic with LMDB writes.
- Retryable LMDB failures no longer risk leaving pointer-file side effects behind.

Alternatives considered:
- Keep transaction arguments for commit existence checks. Rejected because the API shape invites ref mutation during active LMDB write transactions.

### Decision: `create_branch` is the only transaction-aware `HeadOps` method
`create_branch(..., txn=...)` remains the single special case because bootstrap may need to create the initial commit/tree before publishing the branch file. Its transaction-ownership rule matches the current `HeadOps` pattern: `create_branch` closes the transaction if and only if it opened it. Its sequencing requirement is: create bootstrap commit state, finish the transaction that created that commit, then create the branch file.

Rationale:
- Bootstrap is the only legitimate case where `HeadOps` still needs help creating LMDB state before it can publish a ref.
- Keeping this exception narrow avoids spreading transaction-aware pointer behavior across the rest of the API.

Alternatives considered:
- Move bootstrap commit creation out of `HeadOps` entirely. Rejected for now to keep the change focused on ref-publication correctness rather than broader bootstrap API restructuring.

### Decision: affected `IndexOps` mutation paths use optimistic post-transaction publication
For each affected mutation:

1. Read the current base commit through `HeadOps`.
2. Open an LMDB write transaction.
3. Derive the next immutable commit snapshot from that base commit.
4. Close the LMDB transaction successfully.
5. Ask `HeadOps` to CAS the pointer from the expected old commit to the new commit.
6. If CAS fails with `DmlPointerConflictError`, restart using the conflict's `current_commit` as the new base commit instead of rereading from `.dml/refs/**` separately.

Rationale:
- This preserves immutable commit construction while treating branch/index files as optimistic publication selectors.
- `DmlPointerConflictError.current_commit` already provides the minimal state needed for retry.

Alternatives considered:
- Write ref changes first, then LMDB commit. Rejected because it recreates the existing corruption window.
- Journal ref intents inside LMDB. Rejected as unnecessary complexity for this change.

Illustrative shape:

```python
base_commit = head_ops.get_index_commit(index_id)
while True:
    with self._tx(readonly=False) as txn:
        ctx = txn.get_commit_ctx(base_commit)
        new_commit = derive_next_commit(ctx, txn)
    try:
        head_ops.update_index_commit(index_id, base_commit, new_commit)
        return new_commit
    except DmlPointerConflictError as err:
        base_commit = err.current_commit
```

For branch-backed finalization, the same pattern applies after commit derivation, except publication targets `update_branch_commit(...)` and index cleanup happens as a separate `HeadOps.delete_index(index_id)` step after successful publication.

### Decision: index deletion and listing stay simple `HeadOps` operations
`delete_index` remains an unconditional `HeadOps` file-deletion operation, and index listing remains owned entirely by `HeadOps`.

Rationale:
- Once index publication is no longer performed inside `IndexOps`, deletion is no longer part of an optimistic compare-and-swap contract.
- The change is about moving `.dml/refs/local/indexes/**` ownership entirely behind `HeadOps`, not adding extra concurrency semantics to listing or deletion.

Alternatives considered:
- Add compare-and-delete semantics to `delete_index`. Rejected because the desired ownership model treats index deletion as plain `HeadOps` cleanup rather than an `IndexOps` publication step.

### Decision: detached scratch commit helpers do not publish temporary index refs
Builtin execution and failed-execution helper flows build detached scratch commit state directly in LMDB and return the resulting DAG/commit refs without creating temporary index files.

Rationale:
- Temporary index refs exist only to reuse index-mutation helpers, but they inherit the same unsafe publication assumptions.
- Detached scratch commit construction better matches the actual need in those flows.

Alternatives considered:
- Keep temporary index refs and move only their publication outside transactions. Rejected because it preserves needless mutable ref churn and extra retry complexity.

## Risks / Trade-offs

- [Conflict retries can leave unreachable commits from failed publication attempts] -> Accept as a consequence of immutable optimistic publication; later GC can reclaim them.
- [Retry loops may rebuild commits multiple times under contention] -> Keep mutation logic deterministic from `(base_commit, operation args)` and reuse `current_commit` from conflicts to avoid extra ref reads.
- [Temporary-index helper removal may require new internal commit-building helpers] -> Limit new helpers to detached scratch construction and keep them private to `IndexOps`.
- [Bootstrap sequencing in `create_branch` may still be implemented inconsistently] -> Specify that `HeadOps` follows its existing ownership rule: it closes the transaction only when it opened it.

## Migration Plan

1. Narrow `HeadOps` pointer APIs so only `create_branch` remains transaction-aware, while index lookup/list/delete stay fully owned by `HeadOps`.
2. Convert affected `IndexOps` methods to the optimistic derive/commit/CAS/retry loop.
3. Replace temporary index-file helper flows with detached scratch commit construction.
4. Update tests to assert post-transaction publication order and stale-pointer retry behavior.
5. Run targeted internal ops and API tests covering index mutation, commit finalization, and builtin execution helpers.

Rollback strategy:
- Revert the `IndexOps` publication-loop changes and restore prior `HeadOps` method signatures together, because mixed models would be inconsistent.

## Open Questions

- Which existing helper methods in `IndexOps` should be generalized for detached scratch commit construction versus left as mutation-specific code paths?
