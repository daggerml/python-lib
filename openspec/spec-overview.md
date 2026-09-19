# DaggerML Spec Overview

Audience: maintainers and agents working with the repository's OpenSpec capability set.

Use this file to see which documents currently own each high-level concept. It is a governance index for the spec suite, not product documentation.

## Authority Mapping

When a concept is not listed here, treat it as unresolved rather than guessing from proximity or naming.

| concept | authority | scope |
| --- | --- | --- |
| Public API behavior | `docs/start-here/`, `docs/use/artifacts.qmd`, `docs/use/inspection.qmd` | Public Python authoring semantics, node wrappers, DAG-call staging behavior, and user-visible API errors. Public `Projection` inspection belongs to Inspection, not codec normalization. |
| CLI behavior | `docs/use/` and generated `dml --help` | User-visible CLI commands, arguments, and CLI semantics in their owning workflows. |
| Execution and runtime behavior | `docs/use/execution.qmd`, `docs/use/runtimes.qmd`, `src/daggerml/_core/README.md` | Runtime configuration, execution flow, cache behavior, and lifecycle semantics. |
| Cache publication and cache identity | `docs/use/execution.qmd`, `docs/use/runtimes.qmd`, `src/daggerml/_core/README.md` | Runtime cache publication behavior, cache identity, and remote execution state. |
| Storage and object persistence | `docs/use/artifacts.qmd`, `docs/glossary.qmd`, `src/daggerml/_core/README.md` | Storage model, references, GC-adjacent behavior, and external data persistence. |
| Commit and DAG semantics | `docs/use/inspection.qmd`, `docs/use/sharing.qmd`, `src/daggerml/_core/README.md` | Commit objects, DAG model semantics, and repository operations. |
| Remote sync and protocol | `docs/use/sharing.qmd`, `src/daggerml/_core/README.md` | Remote lifecycle, sync protocol semantics, and remote operations behavior. |
| Codec normalization and conversion | `docs/extend/codecs.qmd`, `docs/use/artifacts.qmd` | Codecs owns custom conversion and `ProjectionCodec` internals; Artifacts owns selecting installed codecs for durable values. |
| Extension authoring API | `docs/start-here/funks.qmd`, `docs/extend/adapters.qmd` | `daggerml.contrib.api` decorators, delayed actions, and delayed-lowering chronology. |
| Extension runtime lifecycle | `docs/extend/adapters.qmd`, `docs/extend/executors.qmd`, `src/daggerml/_core/README.md` | Adapters own delayed authoring, runnable resolution, concrete handoff, and transport; Executors own delegated backend-specific construction and lifecycle behavior. |
| Extension plugin packaging and discovery | `docs/extend/`, `src/daggerml/dashboard/README.md` | Adapter, executor, codec, and custom dashboard plugin packaging and discovery behavior. |
| Extension S3 utility behavior | `docs/use/artifacts.qmd` | General `S3Store`, artifact URI, and external-payload behavior. |
| Bundled agent skills | `openspec/specs/bundled-agent-skills/spec.md` | Portable `authoring`, `repository`, and `inspection` guidance resources. |
| Local research dashboard | `src/daggerml/dashboard/README.md`, `dashboard-ui/README.md`, and dashboard OpenSpec capabilities | Dashboard launcher, local HTTP API, read-only projections, cancellation confirmation, redaction, executor introspection, and packaged frontend assets. |
| Dashboard revision navigation | `openspec/specs/dashboard-revision-navigation/spec.md` | Home, canonical project-and-commit routes, revision-scoped reads, refs, and current-versus-historical presentation. |
| Dashboard public API boundary | `openspec/specs/dashboard-public-api-boundary/spec.md` | Allowed core imports and the dashboard's public plugin surface. |
| Dashboard value and runnable inspection | `openspec/specs/dashboard-value-runnable-inspection/spec.md` | Inspector value, runnable, script, and log semantics. |
| Custom DAG dashboards | `openspec/specs/custom-dag-dashboards/spec.md` | Plugin discovery, compatibility, rendering, caching, and declarative result behavior. |

## Handoffs

- Human-facing product docs live under `docs/`.
- Path-based pre-read requirements live in `DOC_MAP.md`.
- Change proposals, designs, and task lists live under `openspec/changes/`.
