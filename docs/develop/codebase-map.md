# Codebase Map

## Primary directories

- `src/daggerml/`: Python package.
- `src/daggerml/_core/`: repository engine, typed object model, execution, and remote transport.
- `src/daggerml/contrib/`: adapters, executors, optional codecs, and extension helpers.
- `src/daggerml/dashboard/`: local dashboard server, read models, API, and
  packaged browser assets.
- `src/daggerml/dashboard/cli.py`: standalone dashboard launcher.
- `dashboard-ui/`: React/TypeScript dashboard source and build configuration.
- `c/`: LMDB-backed storage implementation and vendored native dependencies.
- `tests/`: API, core, and contrib contract and integration tests.
- `examples/`: executable examples and integration fixtures.

## Python package map

| Area | Main modules | Responsibility |
| --- | --- | --- |
| Public Python API | `api.py`, `codecs.py` | Python-friendly DAG authoring, node access, and value staging. |
| CLI | `_cli.py` | Reflects `Dml` and its public namespaces as the `dml` command. |
| Runtime boundary | `_core/dml.py`, `config.py`, `revision.py`, `uri.py` | Resolves configuration and revisions, opens storage, and composes services. |
| Repository operations | `head.py`, `commit.py`, `dag.py`, `index.py` | Pointers, history, immutable DAG reads, mutable DAG construction, and execution. |
| Persistence | `types.py`, `db.pyx`, `serde.py` | Namespaced types, transaction-aware storage, and serialization. |
| Remote operation | `remote.py`, `s3_cas.py`, `exec_state.py` | CAS transfer, refs, cache publication, and cross-process execution state. |
| Research dashboard | `dashboard/` | Read-only repository and remote projections, local HTTP API, event streams, and confirmed cancellation. |

## Where to begin

- A public API behavior: start in `api.py`, then trace its `Dml` namespace call.
- A CLI behavior: start in `_cli.py`; command shape follows public `Dml` methods.
- History, branch, or checkout behavior: inspect `head.py` and `commit.py`.
- DAG construction or function execution: inspect `index.py` with `exec_state.py` and `remote.py`.
- Persisted object shape or reference validation: inspect `types.py` and `db.pyx`.
- Adapter or executor behavior: inspect `contrib/` and its matching tests.
- Dashboard behavior: start with
  `docs/develop/architecture/dashboard.md`, then inspect `dashboard/` and the
  `dashboard-ui/` source.

Use the architecture pages for subsystem boundaries. Detailed repository edit
guidance is maintained outside this documentation path.
