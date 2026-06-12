# Ops Layer

The core repository layer is the runtime's working center. The main classes in `src/daggerml/_core/` each own a narrow slice of behavior, but they all share the same transaction and object model.

## Shared foundation

`DmlDB` and `TxnWithValid` in `types.py` do the shared DB work for the whole layer:

- open read or write transactions through `DmlDB.tx()`,
- serialize and deserialize typed objects through `TxnWithValid.put()` and `TxnWithValid.get()`,
- enforce namespace-aware reads, writes, and object validation.

That lets the higher-level repository classes focus on branch rules, commit structure, execution flow, and remote publication instead of raw LMDB details.

## Pointer and history subsystems

`Head` manages the lightweight filesystem pointers under `.dml/`. It owns:

- the current `HEAD` state,
- local branches under `.dml/refs/local/heads/`,
- mutable index pointers under `.dml/refs/local/indexes/`,
- remote-tracking refs under `.dml/refs/remote/`.

`CommitOps` works one layer below that. It reads and writes `Commit` and `Tree` objects, computes DAG-map diffs, and implements merge, rebase, and revert behavior. In practice, `Head` answers "what commit should I start from?" and `CommitOps` answers "what new commit should exist after this operation?"

## Read-only graph access

`DagOps` is the read side of the repository model.

- `DagOps` describes finished DAGs, resolves named nodes, and returns argv nodes for committed DAG state.

This module is intentionally smaller than `IndexOps` because it does not create new graph state. It mostly interprets committed objects.

## Mutable workspace and execution

`IndexOps` is the busiest subsystem.

An index is DaggerML's mutable workspace: it points at a commit, carries an in-progress DAG, and lets the runtime add literals, imports, function calls, names, results, and errors before turning that state back into a commit.

`IndexOps` also owns function execution. Its flow is roughly:

1. Prepare argv and kwargv nodes in the mutable DAG.
2. Try a built-in function path from `builtins.py`.
3. Check the remote-backed cache through `Remote`.
4. If needed, publish an argv manifest through `Remote`.
5. Coordinate execution through `ExecutionState`.
6. Finish by importing the resulting DAG back into the caller's index.

This is why `IndexOps` depends on many neighbors: `Head` for index pointers, `DagOps` for graph inspection, `Remote` for cache lookup and manifest publication, and `ExecutionState` for resumable execution state.

## Cache, remote sync, and GC

`Remote` is broader than cache publication alone. It handles:

- manifest publication and materialization,
- cache refs and DAG refs,
- branch and tag sync for `dml://` project URIs,
- execution transport state,
- remote prune and mark-and-sweep GC.

Local cleanup stays on `DmlDB.gc()`. It asks the DB layer for objects not reachable from branch, index, or HEAD roots and deletes them in a write transaction.

## Relationship summary

- `Head` points at commits and indexes.
- `CommitOps` changes repository history.
- `IndexOps` creates and mutates in-progress DAG state.
- `DagOps` reads finished graph state.
- `Remote` maps argv identity to remote cache refs and moves object graphs across the network boundary.
- `DmlDB.gc()` removes unreachable local objects.

The common theme is that all of them manipulate the same typed objects and refs. The layer is split by responsibility, not by separate storage backends or separate schemas.
