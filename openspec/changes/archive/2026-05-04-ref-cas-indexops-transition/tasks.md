## 1. HeadOps boundary changes

- [x] 1.1 Narrow `HeadOps` public pointer methods so only `create_branch` remains transaction-aware.
- [x] 1.2 Update `HeadOps.create_branch` sequencing so bootstrap commit creation finishes before the branch file is written.
- [x] 1.3 Keep index lookup/list/delete fully owned by `HeadOps`, with delete remaining unconditional file cleanup.
- [x] 1.4 Adjust or add tests covering txn-free pointer lookup/update/delete behavior and bootstrap branch creation ordering.

## 2. IndexOps optimistic publication

- [x] 2.1 Refactor affected `IndexOps` mutation paths to derive new commits in LMDB and publish them through post-transaction `HeadOps` CAS.
- [x] 2.2 Implement stale-pointer retry loops that restart from `DmlPointerConflictError.current_commit` instead of direct ref-file access.
- [x] 2.3 Update `IndexOps.commit(..., head=...)` so branch advancement occurs only after LMDB commit success and index cleanup happens through `HeadOps.delete_index(...)` after publication.

## 3. Scratch commit helpers and verification

- [x] 3.1 Replace temporary index-ref helper flows in builtin and failed-execution paths with detached scratch commit construction.
- [x] 3.2 Update tests for builtin execution and failed-execution DAG construction so they assert no temporary index refs are published.
- [x] 3.3 Run targeted internal ops and API tests for index mutation, commit finalization, stale-pointer retries, and scratch helper behavior.
