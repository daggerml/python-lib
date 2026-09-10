# DaggerML Spec Overview

Audience: maintainers and agents working with the repository's OpenSpec capability set.

Use this file to see which documents currently own each high-level concept. It is a governance index for the spec suite, not product documentation.

## Authority Mapping

When a concept is not listed here, treat it as unresolved rather than guessing from proximity or naming.

| concept | authority | scope |
| --- | --- | --- |
| Public API behavior | `docs/use/reference/python-authoring.qmd`, `docs/use/concepts/dags-nodes-results.qmd`, `docs/use/reference/errors.qmd` | Public Python authoring semantics, node wrappers, DAG-call staging behavior, and user-visible API errors. |
| CLI behavior | `docs/use/reference/cli.qmd` | User-visible CLI commands, arguments, and CLI semantics. |
| Execution and runtime behavior | `docs/use/reference/configuration.qmd`, `docs/use/concepts/funks-execution-cache.qmd`, `docs/develop/architecture/execution-and-runtime-state.qmd` | Runtime configuration, execution flow, cache behavior, and lifecycle semantics. |
| Cache publication and cache identity | `docs/use/concepts/funks-execution-cache.qmd`, `docs/develop/architecture/execution-and-runtime-state.qmd`, `docs/develop/architecture/remotes-and-sync.qmd` | Runtime cache publication behavior, cache identity, and remote execution state. |
| Storage and object persistence | `docs/use/concepts/artifacts-data-codecs.qmd`, `docs/glossary.qmd`, `docs/develop/architecture/dag-storage-and-types.qmd`, `docs/use/guides/artifacts.qmd` | Storage model, references, GC-adjacent behavior, and external data persistence. |
| Commit and DAG semantics | `docs/use/concepts/history-remotes.qmd`, `docs/use/concepts/dags-nodes-results.qmd`, `docs/develop/architecture/dag-storage-and-types.qmd` | Commit objects, DAG model semantics, and repository operations. |
| Remote sync and protocol | `docs/use/concepts/history-remotes.qmd`, `docs/develop/architecture/remotes-and-sync.qmd` | Remote lifecycle, sync protocol semantics, and remote operations behavior. |
| Codec encoding and import/export behavior | `docs/extend/reference/codec-contracts.qmd`, `docs/use/concepts/artifacts-data-codecs.qmd` | Codec registry behavior, encoding rules, and import/export semantics. |
| Extension authoring API | `docs/use/guides/author-a-dag.qmd`, `docs/extend/concepts/extension-model.qmd` | `daggerml.contrib.api` decorators, delayed actions, and execution helpers. |
| Extension runtime lifecycle | `docs/extend/reference/adapter-operations.qmd`, `docs/extend/reference/executor-lifecycle.qmd`, `docs/develop/architecture/execution-and-runtime-state.qmd` | Adapter/executor pairing, execution-state transitions, and deployment-specific execution behavior. |
| Extension plugin packaging and discovery | `docs/extend/reference/plugin-api.qmd` | Adapter, executor, codec, and custom dashboard plugin packaging and discovery behavior. |
| Extension S3 utility behavior | `docs/use/guides/artifacts.qmd`, `docs/extend/reference/codec-contracts.qmd` | `S3Store`, artifact URI behavior, and dataframe serialization. |
| Bundled agent skills | `openspec/specs/bundled-agent-skills/spec.md` | Portable `authoring`, `repository`, and `inspection` guidance resources. |
| Local research dashboard | `docs/develop/architecture/dashboard.qmd` | Dashboard launcher, local HTTP API, read-only projections, cancellation confirmation, redaction, executor introspection, and packaged frontend assets. |
| Dashboard revision navigation | `openspec/specs/dashboard-revision-navigation/spec.md` | Home, canonical project-and-commit routes, revision-scoped reads, refs, and current-versus-historical presentation. |
| Dashboard public API boundary | `openspec/specs/dashboard-public-api-boundary/spec.md` | Allowed core imports and the dashboard's public plugin surface. |
| Dashboard value and runnable inspection | `openspec/specs/dashboard-value-runnable-inspection/spec.md` | Inspector value, runnable, script, and log semantics. |
| Custom DAG dashboards | `openspec/specs/custom-dag-dashboards/spec.md` | Plugin discovery, compatibility, rendering, caching, and declarative result behavior. |

## Handoffs

- Human-facing product docs live under `docs/`.
- Path-based pre-read requirements live in `DOC_MAP.md`.
- Change proposals, designs, and task lists live under `openspec/changes/`.
