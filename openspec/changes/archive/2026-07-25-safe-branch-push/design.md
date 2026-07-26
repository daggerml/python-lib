## Context

`Dml.push()` currently resolves the local revision and directly calls `Remote.put_ref()`. Branch publication passes `exists_ok=True`, which produces an unconditional S3 overwrite. `Remote.get_ref()` can validate and materialize a remote commit closure into the local database, but it discards the S3 ETag. `CommitOps` already traverses commit parents with a private ancestry helper.

The change must keep `Dml` as a thin API wrapper. A push preflight must download remote objects into the local database only; it must not update local tracking refs, merge commits, or move local branch pointers.

## Goals / Non-Goals

**Goals:**
- Reject non-fast-forward branch updates by default.
- Prevent another branch writer from invalidating the preflight between its remote read and ref publication.
- Provide an explicit `force` option for unconditional branch or tag replacement.
- Preserve create-only tag publication when force is not requested.
- Place commit graph queries in `CommitOps` and remote snapshot/publication behavior in `Remote`.

**Non-Goals:**
- Change pull, fetch, merge, or local-head behavior.
- Add remote branch creation flags or change the remote storage layout.
- Make force pushes race-safe or add server-side branch locks.
- Change cache, active-execution, transport, or other non-project ref update behavior.

## Decisions

### Put branch push orchestration in `Remote`

`Dml.push()` will continue to resolve its API inputs, then delegate branch publication with the resolved commit and `force` flag. `Remote` owns the remote snapshot and S3 precondition because it is the only layer that can retain the ETag needed for the subsequent write. This avoids leaking S3 storage details into `Dml`.

An alternative is to have `Dml` call separate read, ancestry, and write methods. That would make the public wrapper responsible for synchronization policy and make it easy to lose the snapshot token.

### Expose ancestry as a public `CommitOps` query

`CommitOps._is_ancestor()` will become a public database-backed query. `Remote` will invoke it only after the existing remote branch ref has been validated and its commit graph materialized locally. This keeps parent traversal in the module that owns commit semantics and supports merge commits by traversing every parent.

An alternative is to duplicate parent traversal in `Remote`, which would couple remote sync to the `Commit` representation and duplicate existing history logic.

### Treat a remote ref read as an optimistic snapshot

The remote layer will add a private snapshot path that reads the branch manifest as a `CasItem`, validates it, materializes its commit closure, and retains the item's ETag for conditional replacement. The existing public ref-read behavior can remain focused on returning a commit ref.

For a non-forced branch update:

```text
remote missing  -> upload immutable objects -> If-None-Match: *
remote present  -> materialize tip -> require tip <= candidate -> If-Match: ETag
```

Uploading immutable CAS objects before the conditional ref write is safe: objects not reached by a successfully published ref are eligible for normal garbage collection. A failed `If-Match` or `If-None-Match` raises a repository-level push conflict and leaves the remote branch unchanged.

### Define force consistently

`force=True` skips the remote existence read, ancestry check, and conditional ref-write precondition, publishing the selected branch or tag with an unconditional overwrite. Without force, tags remain create-only.

An alternative is to retain ETag validation during force pushes. That is safer under concurrency, but it does not match the requested semantics that force ignores the normal remote checks. A later change can introduce a distinct compare-and-swap force mode if needed.

## Risks / Trade-offs

- [Concurrent normal push] A ref can advance after validation. -> Use the retained ETag with `If-Match`; fail rather than overwrite the newer tip.
- [Concurrent branch creation] Two clients can both observe no remote ref. -> Use `If-None-Match: *`; only one create succeeds.
- [Failed conditional publication leaves uploaded objects] The CAS upload can complete before the ref write fails. -> CAS is immutable and remote GC already removes objects unreachable from refs.
- [Force push loses concurrent history or changes a tag] An unconditional force update can replace a newer remote tip or tag. -> This is explicit, documented force-push behavior.
- [Remote closure cannot be materialized] A malformed or incomplete remote ref prevents ancestry validation. -> Fail before publishing the branch.
