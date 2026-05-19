# Ops Layer

The ops layer is the repository's working core. Each class in `src/daggerml/_internal/ops/` owns a narrow slice of behavior, but they all share the same transaction and object model.

## Shared foundation

`BaseOps` in `base_ops.py` does three jobs for the whole layer:

- opens read or write transactions through `_tx()`,
- serializes and deserializes typed objects through `TxnContext.put()` and `TxnContext.get()`,
- retries recoverable DB failures such as map growth or environment reopen events.

That means the higher-level ops classes can focus on branch rules, commit structure, or execution flow instead of raw LMDB details.

## Pointer and history subsystems

`HeadOps` manages the lightweight filesystem pointers under `.dml/`. It owns:

- the current `HEAD` state,
- local branches under `.dml/refs/local/heads/`,
- mutable index pointers under `.dml/refs/local/indexes/`,
- remote-tracking refs under `.dml/refs/remote/`.

`CommitOps` works one layer below that. It reads and writes `Commit` and `Tree` objects, computes DAG-map diffs, and implements merge, rebase, and revert behavior. In practice, `HeadOps` answers "what commit should I start from?" and `CommitOps` answers "what new commit should exist after this operation?"

## Read-only graph access

`DagOps` and `NodeOps` are the read side of the repository model.

- `DagOps` lists DAGs, describes finished DAGs, and resolves named nodes plus argv and kwargv nodes.
- `NodeOps` reads a node's value and can fully unroll datum refs into plain Python values.

These modules are intentionally smaller than `IndexOps` because they do not create new graph state. They mostly interpret committed objects.

## Mutable workspace and execution

`IndexOps` is the busiest subsystem.

An index is DaggerML's mutable workspace: it points at a commit, carries an in-progress DAG, and lets the runtime add literals, imports, function calls, names, results, and errors before turning that state back into a commit.

`IndexOps` also owns function execution. Its flow is roughly:

1. Prepare argv and kwargv nodes in the mutable DAG.
2. Try a built-in function path from `builtins.py`.
3. Check the remote-backed cache through `CacheOps`.
4. If needed, publish an argv manifest through `RemoteOps`.
5. Coordinate execution through `ExecutionState`.
6. Finish by importing the resulting DAG back into the caller's index.

This is why `IndexOps` depends on many neighbors: `HeadOps` for index pointers, `DagOps` and `NodeOps` for graph inspection, `CacheOps` for cache lookup, and `RemoteOps` for manifest publication and remote state.

## Cache, remote sync, and GC

`CacheOps` is a small but important bridge. Locally, it understands that a cache key comes from the argv datum id. Remotely, it relies on `RemoteOps` to read and publish cache refs. It keeps cache identity logic close to the repository model instead of scattering it through adapters.

`RemoteOps` is much broader. It handles:

- manifest publication and materialization,
- cache refs and DAG refs,
- branch and tag sync for `dml://` project URIs,
- execution invalidation and cancellation metadata stored remotely,
- remote prune and mark-and-sweep GC.

`GcOps` is the local cleanup partner. It asks the DB layer for objects not reachable from branch, index, or HEAD roots and deletes them in a write transaction.

## Relationship summary

- `HeadOps` points at commits and indexes.
- `CommitOps` changes repository history.
- `IndexOps` creates and mutates in-progress DAG state.
- `DagOps` and `NodeOps` read finished graph state.
- `CacheOps` maps argv identity to remote cache refs.
- `RemoteOps` moves object graphs across the network boundary.
- `GcOps` removes unreachable local objects.

The common theme is that all of them manipulate the same typed objects and refs. The layer is split by responsibility, not by separate storage backends or separate schemas.
