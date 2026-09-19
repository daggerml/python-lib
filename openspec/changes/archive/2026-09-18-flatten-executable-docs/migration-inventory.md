## Flat Use Pages

`docs/use/README.qmd` becomes `docs/use/index.qmd`; subgroup landing pages are removed after their links are merged into that index.

| Current path below `docs/use/` | Flat page |
| --- | --- |
| `concepts/projects.qmd` | `projects.qmd` |
| `concepts/dags-nodes-results.qmd` | `dags-nodes-results.qmd` |
| `concepts/funks-execution-cache.qmd` | `funks-execution-cache.qmd` |
| `concepts/runtimes.qmd` | `runtimes.qmd` |
| `concepts/artifacts-data-codecs.qmd` | `artifacts-data-codecs.qmd` |
| `concepts/history-remotes.qmd` | `history-remotes.qmd` |
| `concepts/errors.qmd` | `errors.qmd` |
| `concepts/errors-provenance.qmd` | `errors-provenance.qmd` |
| `guides/author-a-dag.qmd` | `author-a-dag.qmd` |
| `guides/docker-workloads.qmd` | `docker-workloads.qmd` |
| `guides/remote-execution.qmd` | `remote-execution.qmd` |
| `guides/artifacts.qmd` | `artifacts.qmd` |
| `guides/custom-codecs.qmd` | `custom-codecs.qmd` |
| `guides/temporary-projects.qmd` | `temporary-projects.qmd` |
| `guides/runtime-inspection-cancellation.qmd` | `runtime-inspection-cancellation.qmd` |
| `guides/refresh-cache.qmd` | `refresh-cache.qmd` |
| `guides/inspect-a-completed-dag.qmd` | `inspect-a-completed-dag.qmd` |
| `guides/custom-dag-dashboards.qmd` | `custom-dag-dashboards.qmd` |
| `guides/share-reuse.qmd` | `share-reuse.qmd` |
| `reference/cli.qmd` | `cli.qmd` |
| `reference/python-authoring.qmd` | `python-authoring.qmd` |
| `reference/configuration.qmd` | `configuration.qmd` |
| `reference/runtime-state.qmd` | `runtime-state.qmd` |
| `reference/errors.qmd` | `error-reference.qmd` |

## Flat Extend Pages

`docs/extend/index.qmd` remains the section landing page.

| Current path below `docs/extend/` | Flat page |
| --- | --- |
| `concepts/extension-model.qmd` | `extension-model.qmd` |
| `concepts/adapters-and-executors.qmd` | `adapters-and-executors.qmd` |
| `concepts/codecs.qmd` | `codecs.qmd` |
| `concepts/remote-integrations.qmd` | `remote-integrations.qmd` |
| `concepts/plugin-registration.qmd` | `plugin-registration.qmd` |
| `guides/write-adapter.qmd` | `write-adapter.qmd` |
| `guides/write-executor.qmd` | `write-executor.qmd` |
| `guides/write-shared-codec.qmd` | `write-shared-codec.qmd` |
| `guides/package-integration.qmd` | `package-integration.qmd` |
| `guides/test-integration.qmd` | `test-integration.qmd` |
| `reference/adapter-operations.qmd` | `adapter-operations.qmd` |
| `reference/executor-lifecycle.qmd` | `executor-lifecycle.qmd` |
| `reference/codec-contracts.qmd` | `codec-contracts.qmd` |
| `reference/plugin-api.qmd` | `plugin-api.qmd` |
| `reference/built-in-integrations.qmd` | `built-in-integrations.qmd` |

## Develop Disposition

| Current material | Destination |
| --- | --- |
| setup, testing, contributing | root `CONTRIBUTING.md` |
| repository and Python package map | root and `src/daggerml/README.md` |
| core storage, execution, and remote architecture | `src/daggerml/_core/README.md`, `c/README.md`, and current OpenSpec specs |
| public API and CLI architecture | `src/daggerml/README.md` plus flat Use reference pages |
| dashboard architecture | `src/daggerml/dashboard/README.md`, `dashboard-ui/README.md`, flat Use dashboard guidance, and dashboard specs |
| dated flaky-CI investigation | delete after confirming archived changes and tests retain outcomes |
| Develop landing pages | delete |

## Top-Level Examples Disposition

| Material | Destination |
| --- | --- |
| hello world, errors, loaded funks, dagclass, freeze | relevant Start here or flat Use pages; exact contracts remain in API/core tests |
| low-level runtime CLI, history, sync, cache invalidation | flat Use pages with focused executable commands; exhaustive behavior remains in CLI/core integration tests |
| live graph and cancellation | flat runtime inspection page; timing and lifecycle contracts remain in integration tests |
| Docker dataset and Docker dagclass | flat Docker workload page; image/artifact contracts remain in contrib integration tests |
| SSH over Docker | flat remote execution page for reader composition; infrastructure behavior remains in executor integration tests |
| dashboard demo population | dashboard test fixtures and co-located dashboard README |
| dashboard plugin package | dashboard plugin test fixture and flat custom dashboard page |
| Moto lifecycle helper | documentation build support or test fixtures |
| Docker build contexts | documentation build support or contrib test fixtures |
| shell orchestration and shared helpers | delete after docs/tests own behavior |
| uninvoked Docker reload example | delete |

No example file may be deleted until its assigned docs, test, or fixture destination is present.
