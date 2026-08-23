## Context

Remote project refs currently identify a commit whose generic object traversal follows every typed ref, including every commit parent. Fetch writes all missing objects before publishing a local tracking pointer, and locally present objects terminate traversal. Commit operations and local GC consequently assume that every referenced parent exists.

Commit objects already permit content-addressed parent refs without validating local existence. Their trees are complete snapshots rather than deltas, so a selected commit remains useful when older parent commits are omitted as long as its non-history closure is complete. See `proposal.md` and the shallow-history capability spec for the behavioral contract.

## Goals / Non-Goals

**Goals:**

- Preserve immutable object identities and complete selected snapshots.
- Make shallow availability explicit, deterministic, and distinguishable from corruption.
- Support incremental pulls and later deepening without redownloading complete local closures.
- Keep ancestry-dependent mutation and publication conservative when history is insufficient.
- Retain the existing fetch-before-tracking publication order and replayable local database writes.

**Non-Goals:**

- Lazy network fetch when inspection encounters a missing commit.
- Arbitrary object-graph depth limits or partial DAG snapshots.
- Persisted default depth policy on dependency configuration.
- Removing locally available history to make a repository shallower.
- Changing remote descriptors, CAS encoding, typed refs, or immutable DML object schemas.

## Decisions

### Separate commit ancestry from snapshot traversal

Add a project-commit materializer alongside the existing complete generic materializer. It processes commits by generation and materializes each commit's `tree` closure without following `parents` through the generic ref collector. Parent commits are queued according to the requested mode:

```text
selected commit, generation 1
    |-- tree -----------------> complete generic object traversal
    `-- parents --------------> next commit generation or shallow frontier
```

Depth one includes the selected commit. All parents of a merge commit enter the same next generation. This matches the repository's all-parent ancestry operations and avoids silently reducing merge history to first-parent history.

Alternative: add a depth counter to generic remote traversal. Rejected because it would truncate trees, imported DAGs, nodes, or data and produce unusable snapshots.

### Store exact intentionally absent commit refs

Add versioned, atomically replaced repository-local shallow metadata, conceptually `.dml/shallow.json`, with an exact sorted set of absent `commit:` refs. A missing ref is removed when that commit becomes available; omitted parents discovered at a new frontier are added. Existing objects always remain visible even if metadata cleanup is delayed.

Tracking absent refs rather than present boundary commits handles merge commits with independently available parents and lets GC recognize the exact missing leaves. The file stores no endpoint identity: immutable commit identity is source-independent, and explicit fetch source selection remains responsible for later deepening.

Alternative: treat every missing commit as a valid boundary. Rejected because it hides interrupted fetches, corruption, and accidental deletion. Alternative: rewrite boundary commits without parents. Rejected because it changes content-derived identities and remote compatibility.

### Define three traversal modes

Initial fetch without depth follows complete ancestry when it does not encounter an existing local commit. Ordinary update fetch follows new history until an existing local commit and preserves any older shallow entries. `depth=N` walks through existing commits as needed to ensure at least N available generations from the selected tip without deleting deeper objects. `unshallow=True` follows parents through existing frontier commits until reachable history is complete.

`depth` must be positive and is mutually exclusive with `unshallow`. Clone exposes depth but not unshallow because a new clone has no existing boundary. Pull exposes depth; ordinary pull uses update mode and is normally preferable because it connects to the local tip.

Alternative: make omitted depth always mean fully traverse through existing shallow boundaries. Rejected because every routine pull would unexpectedly download all old history.

### Publish objects, shallow metadata, then tracking state

Fetch first downloads and validates all planned objects, then stores them using the replayable database write path. Under the repository-state lock it atomically replaces shallow metadata before replacing the selected tracking ref. New shallow entries name only refs verified absent after object materialization, so publishing metadata first cannot hide available history. If tracking publication fails, downloaded objects and conservative shallow entries may remain but no incomplete tracking tip is exposed.

### Propagate incomplete ancestry explicitly

Internal commit traversal returns both its result and whether an intentionally missing ref prevented a complete answer. Ancestry and merge-base queries therefore have three semantic outcomes: proven true/found, proven false/not found, or unknown because shallow. Callers may proceed when a fact is proven before reaching a boundary; they must not coerce unknown to false.

Log stops and reports truncation. `HEAD~N` and implicit-parent comparison raise fetch/deepening guidance. Ahead/behind returns unavailable counts when incomplete. Merge, rebase, revert, and non-fast-forward validation fail before mutation when their required facts are unknown.

Alternative: graft shallow frontier commits as roots, following Git's presentation in some commands. Rejected because the current merge implementation treats absent merge bases as unrelated histories and could produce an unsafe empty-base merge.

### Extend GC with declared missing leaves

Pass the shallow missing-ref set into local reachability traversal as the only absent refs that may terminate traversal successfully. The database traversal continues to fail on an absent root, tree, DAG, or undeclared commit. After collection, rewrite shallow metadata to remove entries no longer referenced from retained local objects.

Extending the existing native traversal preserves its namespace-wide orphan enumeration and avoids duplicating object serde traversal in Python.

### Restrict publication from shallow history

Available local objects are uploaded while declared absent commit refs terminate collection. A non-forced update is allowed only when ancestry traversal reaches the observed existing remote branch tip before a shallow boundary. That remote tip anchors the omitted complete closure under the remote's existing publication invariant. New-ref creation, forced publication, and updates with unknown ancestry are rejected until unshallowed.

Alternative: check only whether each omitted commit CAS object exists remotely. Rejected because a single object's presence does not prove that its complete closure remains available or anchored against remote garbage collection.

### Keep dependency registration separate from materialization policy

`dep add` continues to write exactly endpoint configuration. Depth belongs to each `fetch --dep` request and does not require a dependency-config migration. This also lets callers deepen one selected branch or tag without changing future fetch defaults.

## Risks / Trade-offs

- [Older clients do not understand shallow metadata and will report missing-object errors] -> Treat shallow repositories as requiring the new client version; unshallow before rollback.
- [Filesystem metadata and LMDB cannot be committed in one transaction] -> Materialize immutable objects first, publish conservative missing-ref metadata second, and update the tracking pointer last.
- [Merge histories can expand each depth generation] -> Preserve all-parent correctness and rely on explicit low depths rather than silently switching to first-parent semantics.
- [A stale shallow entry remains after an interrupted deepen] -> Existing-object checks take precedence, and later fetch or GC normalizes stale entries.
- [Shallow publication is more restrictive than Git] -> Prefer refusal over publishing a remote ref whose complete closure cannot be proven.
- [Log and status payloads change] -> Add fields compatibly where possible and update generated CLI serialization and contract tests together.

## Migration Plan

1. Introduce shallow metadata parsing/writing and availability-aware traversal while treating absent metadata as complete-history mode.
2. Add commit-specific depth materialization and fetch options without changing default initial-fetch behavior.
3. Make history, status, GC, pull, and push consume shallow metadata before enabling depth options publicly.
4. Document that rollback requires unshallowing all retained refs and removing the then-empty shallow metadata file.

No remote migration is required because remote refs and CAS closures remain complete and unchanged.
