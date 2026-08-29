## 1. Commit And Remote Operations

- [x] 1.1 Expose a database-backed `CommitOps` ancestry query that traverses all commit parents.
- [x] 1.2 Add a remote branch-ref snapshot path that validates and materializes an existing commit closure while retaining its S3 ETag.
- [x] 1.3 Implement remote branch publication that uses create-only writes for absent non-forced refs, ancestry plus ETag-conditional writes for existing non-forced refs, and unconditional writes for forced refs.
- [x] 1.4 Preserve create-only tag publication without force, allow forced tag replacement, and translate conditional-write conflicts into a user-facing repository error.

## 2. Public Push Surface

- [x] 2.1 Add keyword-only `force: bool = False` to `Dml.push()` and delegate branch publication to the remote operation layer without adding synchronization policy to `Dml`.
- [x] 2.2 Keep the existing revision resolution, deletion behavior, and tag behavior unchanged apart from passing the force option through publication.

## 3. Tests And Documentation

- [x] 3.1 Add commit-operation coverage for ancestry across linear and merge histories.
- [x] 3.2 Add remote sync integration coverage for safe initial branch creation, fast-forward updates, rejected divergent updates, and forced branch replacement.
- [x] 3.3 Add remote sync coverage for branch-creation and ETag-update races, asserting that the newer remote ref is not overwritten.
- [x] 3.4 Add regression coverage that a push preflight materializes the remote closure without moving local HEAD, local branch refs, or remote-tracking refs, and that force replaces an existing tag.
- [x] 3.5 Update public push and remote-protocol documentation to describe `force`, non-fast-forward rejection, remote-tip download semantics, and create-only non-forced tags.
- [x] 3.6 Run the project's required formatting, type-checking, linting, and non-slow test suite.
