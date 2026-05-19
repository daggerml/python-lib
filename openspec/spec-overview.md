# DaggerML Spec Overview

Audience: maintainers and agents working with the repository's OpenSpec capability set.

Use this file to see which documents currently own each high-level concept. It is a governance index for the spec suite, not product documentation.

## Authority Mapping

When a concept is not listed here, treat it as unresolved rather than guessing from proximity or naming.

| concept | authority | scope |
| --- | --- | --- |
| Public API behavior | `docs/reference/python-api.md`, `docs/concepts/dags-and-nodes.md`, `docs/reference/errors.md` | Public Python API semantics, default-runtime helpers, node-wrapper selection, DAG-call staging behavior, and user-visible API errors. |
| CLI behavior | `docs/reference/cli.md` | User-visible CLI commands, arguments, and CLI semantics. |
| Execution and runtime behavior | `docs/reference/configuration.md`, `docs/concepts/execution.md`, `docs/architecture/remote-protocol.md` | Runtime configuration, execution flow, adapter-boundary payloads, and execution lifecycle semantics. |
| Cache publication and cache identity | `docs/concepts/execution.md`, `docs/architecture/ops-layer.md`, `docs/architecture/remote-protocol.md` | Runtime cache publication behavior, argv-derived cache identity, and the remote records that preserve execution state. |
| Storage and object persistence | `docs/concepts/storage.md`, `docs/concepts/refs-and-namespaces.md`, `docs/architecture/storage-internals.md`, `docs/guides/store-and-load-external-data.md` | Storage model, reference handling, GC-adjacent storage behavior, and external data persistence semantics. |
| Commit and DAG semantics | `docs/concepts/commits-and-history.md`, `docs/concepts/dags-and-nodes.md`, `docs/architecture/ops-layer.md` | Commit objects, DAG model semantics, and the operation-layer responsibilities that create and read them. |
| Remote sync and protocol | `docs/concepts/remotes.md`, `docs/architecture/remote-protocol.md`, `docs/architecture/ops-layer.md` | Remote lifecycle, remote schemas, sync protocol semantics, and remote operations behavior. |
| Codec encoding and import/export behavior | `docs/concepts/codecs-and-values.md` | Codec registry behavior, encoding rules, and import/export semantics. |
| Contrib API surface | `docs/contrib/reference/python-api.md` | `daggerml.contrib.api` decorators, delayed actions, and execution helpers. |
| Contrib literal codecs and dataframe serialization | `docs/contrib/reference/s3-and-codecs.md` | Contrib-owned codec behavior and dataframe serialization semantics. |
| Contrib prebuilt funks | `docs/contrib/reference/python-api.md` | Contrib-owned prebuilt function contracts. |
| Contrib testing helpers | `docs/contrib/reference/python-api.md` | Testing helpers intended for author-code unit tests. |
| Contrib runtime lifecycle | `docs/contrib/concepts/runtime.md`, `docs/contrib/architecture/execution-flow.md`, `docs/contrib/architecture/supervisor-and-state.md` | Supervisor launch, executor start/poll/cleanup, `ExecutionState` transitions, adapter/executor pairing, and deployment-specific execution-graph behavior. |
| Contrib plugin packaging and discovery | `docs/contrib/reference/runtime-surfaces.md` | Adapter and executor registry contracts, plugin packaging, and discovery behavior. |
| Contrib runtime diagnostics and status surfaces | `docs/contrib/reference/runtime-surfaces.md` | Contrib runtime status and diagnostics APIs and registration visibility. |
| Contrib S3 utility behavior | `docs/contrib/reference/s3-and-codecs.md` | `S3Store`, S3 URI normalization, content-addressed S3 object helpers, JSON helpers, tar helpers, and extraction safety rules. |

## Handoffs

- Human-facing product docs live under `docs/`.
- Path-based pre-read requirements live in `DOC_MAP.md`.
- Change proposals, designs, and task lists live under `openspec/changes/`.
