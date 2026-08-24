# DaggerML Spec Overview

Audience: maintainers and agents working with the repository's OpenSpec capability set.

Use this file to see which documents currently own each high-level concept. It is a governance index for the spec suite, not product documentation.

## Authority Mapping

When a concept is not listed here, treat it as unresolved rather than guessing from proximity or naming.

| concept | authority | scope |
| --- | --- | --- |
| Public API behavior | `docs/use/reference/python-authoring.md`, `docs/use/concepts/dags-nodes-results.md`, `docs/use/reference/errors.md` | Public Python authoring semantics, node wrappers, DAG-call staging behavior, and user-visible API errors. |
| CLI behavior | `docs/use/reference/cli.md` | User-visible CLI commands, arguments, and CLI semantics. |
| Execution and runtime behavior | `docs/use/reference/configuration.md`, `docs/use/concepts/funks-execution-cache.md`, `docs/develop/architecture/execution-and-runtime-state.md` | Runtime configuration, execution flow, cache behavior, and lifecycle semantics. |
| Cache publication and cache identity | `docs/use/concepts/funks-execution-cache.md`, `docs/develop/architecture/execution-and-runtime-state.md`, `docs/develop/architecture/remotes-and-sync.md` | Runtime cache publication behavior, cache identity, and remote execution state. |
| Storage and object persistence | `docs/use/concepts/artifacts-data-codecs.md`, `docs/glossary.md`, `docs/develop/architecture/dag-storage-and-types.md`, `docs/use/guides/artifacts.md` | Storage model, references, GC-adjacent behavior, and external data persistence. |
| Commit and DAG semantics | `docs/use/concepts/history-remotes.md`, `docs/use/concepts/dags-nodes-results.md`, `docs/develop/architecture/dag-storage-and-types.md` | Commit objects, DAG model semantics, and repository operations. |
| Remote sync and protocol | `docs/use/concepts/history-remotes.md`, `docs/develop/architecture/remotes-and-sync.md` | Remote lifecycle, sync protocol semantics, and remote operations behavior. |
| Codec encoding and import/export behavior | `docs/extend/reference/codec-contracts.md`, `docs/use/concepts/artifacts-data-codecs.md` | Codec registry behavior, encoding rules, and import/export semantics. |
| Extension authoring API | `docs/use/guides/author-a-dag.md`, `docs/extend/concepts/extension-model.md` | `daggerml.contrib.api` decorators, delayed actions, and execution helpers. |
| Extension runtime lifecycle | `docs/extend/reference/adapter-operations.md`, `docs/extend/reference/executor-lifecycle.md`, `docs/develop/architecture/execution-and-runtime-state.md` | Adapter/executor pairing, execution-state transitions, and deployment-specific execution behavior. |
| Extension plugin packaging and discovery | `docs/extend/reference/plugin-api.md` | Adapter, executor, and codec plugin packaging and discovery behavior. |
| Extension S3 utility behavior | `docs/use/guides/artifacts.md`, `docs/extend/reference/codec-contracts.md` | `S3Store`, artifact URI behavior, and dataframe serialization. |
| Bundled agent skills | `openspec/specs/bundled-agent-skills/spec.md` | Portable `authoring`, `repository`, and `inspection` guidance resources. |

## Handoffs

- Human-facing product docs live under `docs/`.
- Path-based pre-read requirements live in `DOC_MAP.md`.
- Change proposals, designs, and task lists live under `openspec/changes/`.
