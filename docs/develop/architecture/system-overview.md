# System Overview

DaggerML keeps user-facing entrypoints thin and centralizes repository rules in
`daggerml._core`.

1. `daggerml.api` provides Python DAG and node wrappers; `_cli.py` provides the
   `dml` command.
2. `Dml` in `_core/dml.py` resolves configuration, opens the local database, and
    exposes namespaces for runtime, DAG, history, configuration, and bundled skills.
3. `Head`, `CommitOps`, `DagOps`, and `IndexOps` implement repository behavior.
4. `DmlDB`, the Cython database wrapper, and the typed objects in `types.py`
   persist the local graph.
5. `Remote` and `ExecutionState` provide S3-backed object transfer, cache, and
   coordination when a remote is configured.

The local repository stores immutable typed objects in LMDB. Files under
`.dml/` select live repository state through `HEAD`, local branch and runtime
pointers, remote-tracking refs, and configuration. A committed history follows
`Commit -> Tree -> Dag -> Node -> Datum/Error`.

Execution begins while constructing a mutable runtime. Built-in operations run
locally; adapter-backed runnables use normalized DaggerML data as a cache
identity, then use the remote layer for cached results and execution coordination.
