# Internal Modules

Most of DaggerML's implementation lives under `src/daggerml/`, but the important architectural boundary is not "public package vs everything else." It is "ergonomic wrappers vs transactional repository core."

## Top-level package map

- `api.py`: the main Python-facing interface. It exposes the friendly `Dag` wrapper, default-runtime helpers, and node objects that feel natural in Python code.
- `codecs.py`: value staging and loading for Python objects that do not map directly onto the internal dataclasses.
- `_cli.py`: the CLI surface. It mostly translates command-line intent into `Dml` method calls.
- `_core/`: the repository engine.
- `contrib/`: adapters, executors, codecs, and helper surfaces that extend the core runtime.

## The `_core` package

`_core` is where DaggerML stops looking like a user library and starts looking like a small versioned object database with execution support.

### Runtime and context

- `dml.py`: defines `Dml`, the central orchestration object. It exposes namespaces such as `runtime`, `dag`, `config`, and `admin`, opens the DB, and composes repository helpers for each request.
- `config.py`: config models, repo/bootstrap helpers, and validation.
- `revision.py`: normalized revision parsing for `HEAD`, `HEAD~N`, names, commit refs, and project URIs.
- `uri.py`: the parser and validator for `dml://owner/project#branch` and `@tag` selectors.

### Domain model

- `types.py`: the typed object model for everything persisted locally: datums, runnable specs, errors, nodes, DAGs, trees, and commits.
- `builtins.py`: built-in function implementations used before the runtime falls back to adapter execution.

### Repository subsystems

- `head.py`: HEAD state, branches, indexes, tags, and tracking refs on disk.
- `commit.py`: commit creation, history walking, merge, rebase, revert, and DAG-map diffs.
- `index.py`: mutable workspaces, DAG building, function execution, cancellation, and cache publication.
- `dag.py`: read access to finished DAGs and named nodes.
- `remote.py`: remote CAS, refs, manifests, project sync, cache transport, and remote GC.

### Persistence and execution helpers

- `db.pyx`: the Cython wrapper around the C LMDB layer.
- `types.py`: the typed object model plus the `DmlDB` facade and transaction wrappers.
- `serde.py`: DML string serialization and deserialization helpers.
- `exec_state.py`: S3-backed execution lock, launch state, execution state, lineage edges, and adapter IO helpers.
- `s3_cas.py`: the S3-backed content-addressed storage helper used by `remote.py`.
- `util.py`: timestamps, IDs, and smaller shared helpers.

## Why the split looks this way

The public API is intentionally thin. It does not reimplement repository behavior; it mostly stages values and delegates. That keeps the rules for storage, history, caching, and sync in one place: the internal runtime and ops layer.

The `_core` package is also split so that contributors can reason locally:

- if a change is about persisted shape, look at `types.py` and `db.pyx`,
- if it is about repo behavior, look at `head.py`, `commit.py`, `dag.py`, or `index.py`,
- if it is about how callers discover or target state, look at `dml.py`, `config.py`, `revision.py`, and `uri.py`,
- if it is about remote execution coordination, look at `remote.py` and `exec_state.py` together.

The next page, [ops layer](ops-layer.md), covers how those subsystems interact in practice.
