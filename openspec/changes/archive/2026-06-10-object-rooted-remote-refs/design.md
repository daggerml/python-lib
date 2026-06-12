## Context

The current remote path is manifest-first:

```text
ref path -> manifest JSON -> synthetic closure -> CAS blobs
```

The local repository model is object-first:

```text
typed ref -> stored object -> more typed refs
```

That mismatch is the real problem. The remote layer is carrying extra structure that the object model already knows how to express.

## Goals / Non-Goals

**Goals:**

- Make remote refs match the local typed-ref model.
- Remove special-case manifest closure logic.
- Make push, pull, and remote GC traverse the real object graph.
- Keep the payload shape minimal and obvious.
- Keep the breaking change clean: no compat paths, no dual formats, no schema bump.

**Non-Goals:**

- Do not preserve manifest refs in parallel.
- Do not add migration code.
- Do not add richer ref payloads than needed for root identity and metadata.

## Decisions

### Remote Refs Are Simple Typed Pointers

Every remote ref payload should have the same shape:

```json
{
  "ref": {"to": "commit:<oid>"},
  "created": 1718000000,
  "metadata": {}
}
```

`ref.to` is the durable pointer. `created` is write time. `metadata` is the only extension point.

`metadata` is unconstrained globally, but individual ref families may require specific fields. For this change, `active` and `cache` require `metadata.execution_id`. Project refs do not add required metadata beyond the base shape.

Alternative considered: keep `kind`, `schema`, `target`, or closure-adjacent summary fields. Rejected because they duplicate information the path and root object type already provide.

### Push Publishes Objects, Then The Ref

Push should work like this:

```text
root ref
  |
  v
read local object
  |
  v
walk direct typed refs recursively
  |
  v
upload missing CAS objects
  |
  v
write remote ref payload
```

Traversal is uniform. `commit` and `dag` are not special remote layers.

Alternative considered: keep per-root manifest assembly for project refs only. Rejected because it reintroduces two models for the same object graph.

### Pull Materializes By Recursive Object Loading

Pull should start from a typed remote ref, not a manifest. The runtime reads `ref.to`, fetches the CAS object if it is not already local, decodes its direct refs, and repeats until the reachable graph is present locally.

Stopping condition is simple: if a target object already exists in the local DB, traversal does not need to reinsert it.

### Root Type Comes From Ref Family

The remote path determines which root namespaces are valid:

- `refs/projects/...` -> `commit`
- `refs/cache/...` -> `dag`
- `refs/transport/...` -> `dag`
- `refs/active/...` -> `node-argv`
- `refs/tombstone/...` -> any of the above

This keeps validation narrow and local.

### `active` Keeps One Meaning

`active` should always mean "the argv root for the currently coordinated execution." It should not later change into a DAG pointer. Terminal results belong in `cache` or `transport`.

### Remote Liveness Follows The Object Graph

Remote GC should mark from published remote refs by traversing actual stored objects recursively, then sweep CAS objects not found in that traversal. This removes the current dependency on synthetic closure fields and fixes the mismatch between liveness and the real object graph.

### Tombstones Are Moved Refs

Deleting a ref should move the original remote ref payload to the tombstone location without changing its contents. The tombstone is the deleted ref under a new path, not a rewritten record.

Deletion metadata is therefore preserved exactly as it existed on the live ref. This change does not add tombstone-specific wrapper fields or mutation steps.

### Implementation Scope Stays In `remote.py`

This ref-model rewrite should be implementable entirely inside `src/daggerml/_core/remote.py`.

If implementation appears to require code changes outside `remote.py`, that should be treated as a likely bug or mistaken assumption and work should stop for review before proceeding.

## Data Model Sketch

```text
refs/projects/alice/demo/heads/main.json -> commit:c1
refs/cache/ck1.json                     -> dag:d7
refs/active/ck1.json                    -> node-argv:a2
refs/transport/e9.json                  -> dag:d9
refs/tombstone/t1.json                  -> commit:c1 | dag:d7 | node-argv:a2

CAS object graph:

commit:c1 -> tree:t1 -> dag:d1 -> node:n1 -> datum:x1
                         |
                         +-> node:n2 -> dag:d2 -> ...
```

## Risks / Trade-offs

- Breaking raw remote payload compatibility: accepted, since v0.alpha explicitly allows cleanup.
- Recursive traversal now depends entirely on correct direct-ref extraction from stored objects: accepted, because that is the actual data model and is easier to reason about than synthetic closure generation.
- GC now needs the same traversal discipline as pull: accepted, because one graph walk model is simpler than separate manifest and object liveness models.

## Migration Plan

1. Replace manifest payload assumptions in specs with typed-pointer payloads.
2. Rework remote traversal around direct object refs for push, pull, and GC.
3. Align execution-state remote refs so `active`, `cache`, and `transport` each have one stable root type.
4. Rewrite tests to assert object-rooted behavior only.

## Open Questions

- None currently.
