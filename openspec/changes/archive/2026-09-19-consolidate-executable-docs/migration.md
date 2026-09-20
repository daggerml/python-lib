# Documentation Migration Checklist

## Use

| Source | Destination |
| --- | --- |
| `use/index.qmd` | Docs home plus dashboard section inventory |
| `use/projects.qmd` | `use/projects.qmd` |
| `use/configuration.qmd` | `use/projects.qmd` |
| `use/temporary-projects.qmd` | `use/projects.qmd` |
| `use/artifacts-data-codecs.qmd` | `use/artifacts.qmd` for durable values, URIs, external payloads, and installed-codec selection; `use/inspection.qmd` for Projection use; `extend/codecs.qmd` for codec implementation and `ProjectionCodec` mechanics |
| `use/artifacts.qmd` | `use/artifacts.qmd` |
| `use/custom-codecs.qmd` | `extend/codecs.qmd` |
| `use/author-a-dag.qmd` | Start here course, `use/execution.qmd`, and Sharp bits and security |
| `use/dags-nodes-results.qmd` | `use/inspection.qmd`, including committed-DAG Projection traversal, value/context, and reuse |
| `use/funks-execution-cache.qmd` | `use/execution.qmd`, `use/runtimes.qmd`, and the core README |
| `use/docker-workloads.qmd` | `use/execution.qmd` |
| `use/remote-execution.qmd` | `use/execution.qmd` |
| `use/inspect-a-completed-dag.qmd` | `use/inspection.qmd` |
| `use/errors.qmd` | `use/inspection.qmd` |
| `use/error-reference.qmd` | `use/inspection.qmd` and workflow-local failure guidance |
| `use/errors-provenance.qmd` | `use/inspection.qmd` |
| `use/runtimes.qmd` | `use/runtimes.qmd` |
| `use/runtime-inspection-cancellation.qmd` | `use/runtimes.qmd` |
| `use/runtime-state.qmd` | `use/runtimes.qmd` and the core README |
| `use/refresh-cache.qmd` | `use/runtimes.qmd` |
| `use/history-remotes.qmd` | `use/sharing.qmd` |
| `use/share-reuse.qmd` | `use/sharing.qmd` |
| `use/cli.qmd` | Workflow-local CLI guidance and generated `dml --help` |
| `use/python-authoring.qmd` | Start here and workflow-local Python guidance |
| `use/custom-dag-dashboards.qmd` | Dashboard server README |

## Extend

| Source | Destination |
| --- | --- |
| `extend/index.qmd` | Docs home plus dashboard section inventory |
| `extend/codecs.qmd` | `extend/codecs.qmd` |
| `extend/codec-contracts.qmd` | `use/inspection.qmd` for Projection use; `extend/codecs.qmd` for codec implementation and `ProjectionCodec` mechanics |
| `extend/write-shared-codec.qmd` | `extend/codecs.qmd` |
| `extend/extension-model.qmd` | `extend/adapters.qmd`, including delayed authoring/lowering chronology |
| `extend/adapters-and-executors.qmd` | `extend/adapters.qmd` and `extend/executors.qmd` |
| `extend/adapter-operations.qmd` | `extend/adapters.qmd` |
| `extend/write-adapter.qmd` | `extend/adapters.qmd` |
| `extend/executor-lifecycle.qmd` | `extend/executors.qmd` |
| `extend/write-executor.qmd` | `extend/executors.qmd` |
| `extend/remote-integrations.qmd` | `extend/executors.qmd` and the core README |
| `extend/built-in-integrations.qmd` | `extend/codecs.qmd` and `extend/executors.qmd` |
| `extend/plugin-api.qmd` | Three mechanism pages and dashboard server README |
| `extend/plugin-registration.qmd` | Three mechanism pages and dashboard server README |
| `extend/package-integration.qmd` | Three mechanism pages |
| `extend/test-integration.qmd` | Three mechanism pages |
