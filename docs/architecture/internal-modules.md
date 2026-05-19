# Internal Modules

Most of DaggerML's implementation lives under `src/daggerml/`, but the important architectural boundary is not "public package vs everything else." It is "ergonomic wrappers vs transactional repository core."

## Top-level package map

- `api.py`: the main Python-facing interface. It exposes the friendly `Dag` wrapper, default-runtime helpers, and node objects that feel natural in Python code.
- `codecs.py`: value staging and loading for Python objects that do not map directly onto the internal dataclasses.
- `_cli.py`: the CLI surface. It mostly translates command-line intent into `Dml` method calls.
- `_internal/`: the repository engine.
- `contrib/`: adapters, executors, codecs, and helper surfaces that extend the core runtime.

## The `_internal` package

`_internal` is where DaggerML stops looking like a user library and starts looking like a small versioned object database with execution support.

### Runtime and context

- `dml.py`: defines `Dml`, the central orchestration object. It exposes namespaces such as `runtime`, `dag`, `config`, and `admin`, opens the DB, and composes ops objects for each request.
- `dml_context.py`: resolves runtime context from config and environment, including project paths, remote roots, and user identity.
- `config.py`: config models and validation.
- `dml_resolution.py`: revision, DAG, and node selector resolution used by the user-facing commands.
- `revision_uri.py`: the parser and validator for `dml://owner/project#branch` and `@tag` selectors.

### Domain model

- `types.py`: the typed object model for everything persisted locally: datums, runnable specs, errors, nodes, DAGs, trees, and commits.
- `builtins.py`: built-in function implementations used before the runtime falls back to adapter execution.

### Repository subsystems

- `ops/base_ops.py`: shared transaction, object IO, and retry machinery.
- `ops/head.py`: HEAD state, branches, indexes, and tracking refs on disk.
- `ops/commit.py`: commit creation and history operations.
- `ops/index.py`: mutable workspaces, DAG building, function execution, cancellation, and cache publication.
- `ops/dag.py`: read access to finished DAGs.
- `ops/node.py`: value retrieval and datum unrolling.
- `ops/cache.py`: the cache-facing bridge between local argv refs and remote cache refs.
- `ops/remote.py`: remote CAS, refs, manifests, project sync, execution invalidation, and remote GC.
- `ops/gc.py`: local orphan discovery and deletion.
- `ops/config.py`: config file editing. This one sits near the ops layer conceptually, but it works on TOML files rather than LMDB transactions.

### Persistence and execution helpers

- `_db.pyx`: the Cython wrapper around the C LMDB layer. It handles typed value conversion, transactions, object iteration, and orphan listing.
- `exec_state.py`: S3-backed execution lock, launch state, execution state, lineage edges, and adapter IO helpers.
- `execution_context.py`: contextvars for the current execution id and cache key.
- `util.py`: timestamps, IDs, and smaller shared helpers.

## Why the split looks this way

The public API is intentionally thin. It does not reimplement repository behavior; it mostly stages values and delegates. That keeps the rules for storage, history, caching, and sync in one place: the internal runtime and ops layer.

The `_internal` package is also split so that contributors can reason locally:

- if a change is about persisted shape, look at `types.py` and `_db.pyx`,
- if it is about repo behavior, look at `ops/`,
- if it is about how callers discover or target state, look at `dml.py`, `dml_context.py`, and the resolution helpers,
- if it is about remote execution coordination, look at `ops/remote.py` and `exec_state.py` together.

The next page, [ops layer](ops-layer.md), covers how those subsystems interact in practice.
