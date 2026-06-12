# System Overview

DaggerML has a small public surface and a fairly dense internal core.

At the top, `daggerml.api` gives Python users a convenient interface built around `Dml`, `Dag`, and `Node`. The CLI layers on top of the same runtime ideas through `src/daggerml/_cli.py`. Under that, `daggerml._core.dml.Dml` is the main orchestration boundary: it resolves config, opens the database, and routes requests to narrower subsystems.

The internals are organized in four broad layers:

1. Public entrypoints: `daggerml.api`, `src/daggerml/_cli.py`, and the codec helpers in `daggerml.codecs`.
2. Runtime orchestration: `src/daggerml/_core/dml.py`, plus config and revision-resolution helpers in `_core/config.py`, `_core/revision.py`, and `_core/uri.py`.
3. Repository subsystems: `src/daggerml/_core/head.py`, `commit.py`, `dag.py`, `index.py`, and `remote.py`.
4. Persistence and typed state: `src/daggerml/_core/types.py`, `src/daggerml/_core/db.pyx`, `src/daggerml/_core/serde.py`, and `src/daggerml/_core/exec_state.py`.

## How a local write flows

Creating or updating a DAG usually starts in `daggerml.api`. The high-level `Dag` wrapper stages Python values through `daggerml.codecs`, then calls runtime methods on `Dml.runtime`. Those runtime methods create a `DmlDB` handle and delegate to `IndexOps`, `DagOps`, `CommitOps`, `Head`, or `Remote` depending on the job.

Inside the core layer, work happens inside explicit LMDB transactions. `DmlDB.tx()` yields typed `TxnWithValid` wrappers for object reads and writes, while the concrete repository modules apply domain rules on top of that shared machinery: branch pointers in `Head`, history in `CommitOps`, mutable workspaces and execution in `IndexOps`, and remote publication in `Remote`.

The storage layer is deliberately simple: objects live in namespace-partitioned LMDB tables and are addressed by `Ref("namespace:id")`. The filesystem around `.dml/` stores lightweight pointers such as `HEAD` and branch or index refs, while the object graph itself stays in LMDB.

## How execution flows

Function execution is centered in `IndexOps.start_fn()`. It prepares an argv node, tries built-in execution first, then checks the remote-backed cache through `Remote`. If there is no cached result, it publishes an argument manifest through `Remote`, coordinates a run with `ExecutionState`, and waits for either a finished cached DAG or an in-progress execution to resume later.

That split is important to the architecture:

- local LMDB storage keeps the typed repository state,
- remote storage carries cache refs, manifests, and transport state,
- `ExecutionState` in `exec_state.py` handles the advisory lock and execution lineage needed for async adapters.

## How history flows

Committed repository state is modeled as `Commit -> Tree -> Dag -> Node -> Datum/Error`. `Head` points the working repo at a commit through `HEAD`, branch refs, and index refs. `CommitOps` creates new commits, walks history, and handles merge, rebase, and revert logic. `DagOps` then gives read access to the objects inside those commits.

## How remote sync fits in

Remote sync is not a separate storage engine. It is a transport and publication layer around the local object graph. `Remote` takes local objects, builds manifests, uploads missing CAS objects to S3, and publishes refs under the remote `refs/` tree. Pulling does the reverse: resolve a remote ref, fetch the needed manifests and CAS objects, materialize them locally, then update the appropriate tracking pointer.

For more detail, continue with [internal modules](internal-modules.md) and [ops layer](ops-layer.md).
