# Documentation Migration Inventory (Task 4.1)

Maintainer evidence, not product documentation. Snapshot: 2026-09-07. Baseline is
Git commit `508242f8459a2ca818b9da9da9568a6a5c769854` (`HEAD` when inspected), not
the index: the index and worktree already contain extensive migration work.
Current paths and line numbers below describe the inspected worktree. This file
does not certify renders, native tests, distribution contents, or other tasks.
It does not change the task 4.1 checkbox.

## Scope And Classification

- The baseline `git ls-tree -r --name-only HEAD docs` contains exactly 62 files,
  all human-facing Markdown pages. Every one is mapped below, including landing
  pages, duplicate-topic pages, and the dated CI investigation. None is silently
  dropped. The two exceptional renames are `docs/README.md` to `docs/index.qmd`
  and `docs/extend/README.md` to `docs/extend/index.qmd`; other stems are retained.
- The baseline has 93 fenced blocks: 51 Python, 24 Bash, seven text, six TOML,
  two JSON, one HTTP, one sh, and one Mermaid. Inline code is prose/reference
  notation, not a separate executable block. Block locations below identify
  each original and each current authored fence, rather than counting a whole
  page as one example.
- `E` means an executable QMD cell, not proof that it executed successfully.
  Python uses reticulate; Bash runs directly in knitr. `H` means executable but
  hidden. `S` means a `dml-source` inclusion of a canonical Python file.
- `P` means explicitly marked `docs:pseudocode` in the current source. This is
  an observed classification, not blanket approval of the exemption. The
  baseline fences were ordinary static Markdown; do not retroactively claim
  that they were explicitly classified or verified there.
- `O` means documented output/trace, not authored runnable source; `D` means a
  non-code diagram. Current O/D fences also have pseudocode comments, but are
  counted separately here. JSON/TOML/HTTP examples are not O merely because they
  are not Python. Literal data/config validation gaps are called out below.
- `none` accounts explicitly for pages without fenced blocks. In the block
  column, `language baseline-line>current-line class` identifies the opening
  fence in the row's original/current files. Comma-separated pairs share the
  indicated language and class. `new>line` is an added block.
- Canonical source for ordinary page snippets is the current QMD itself. Only
  S inclusions below have canonical downloadable `.py` files; a related old
  example is not implicitly a download or an executable copy of a page block.

## Original Page And Block Ledger

Paths are repository-relative. Prerequisites describe what the examples teach
or would need to execute, not fixtures proven available by this inventory.
`Local` below means Python 3.11+, installed DaggerML, an initialized disposable
project, writable configured `remote.root`, and isolated config. `Script` adds
file-backed inspectable source and local worker execution with access to that
remote. Pages with no code need no execution fixture.

| Original page | Current QMD | Blocks: language original>current class | Prerequisites / classification rationale |
| --- | --- | --- | --- |
| `docs/README.md` | `docs/index.qmd` | none | Product/audience navigation; Examples link added. |
| `docs/develop/README.md` | `docs/develop/README.qmd` | none | Contributor landing; retain architecture and investigation navigation. |
| `docs/develop/architecture/README.md` | `docs/develop/architecture/README.qmd` | none | Architecture landing. |
| `docs/develop/architecture/dag-storage-and-types.md` | `docs/develop/architecture/dag-storage-and-types.qmd` | none | Storage/type architecture prose. |
| `docs/develop/architecture/dashboard.md` | `docs/develop/architecture/dashboard.qmd` | text 41>47,126>136 P; json 237>250,247>261 P; http 295>310 P | Launcher synopsis, route grammar, collection/error envelopes, cancellation request sample. Data/wire validation needs review, not a live cancellation fixture. |
| `docs/develop/architecture/execution-and-runtime-state.md` | `docs/develop/architecture/execution-and-runtime-state.qmd` | none | Execution architecture prose. |
| `docs/develop/architecture/public-api-and-cli.md` | `docs/develop/architecture/public-api-and-cli.qmd` | none | API/CLI boundary prose. |
| `docs/develop/architecture/remotes-and-sync.md` | `docs/develop/architecture/remotes-and-sync.qmd` | none | Synchronization architecture prose. |
| `docs/develop/architecture/system-overview.md` | `docs/develop/architecture/system-overview.qmd` | none | Global layer/boundary reference. |
| `docs/develop/codebase-map.md` | `docs/develop/codebase-map.qmd` | none | Source-tree orientation. |
| `docs/develop/contributing.md` | `docs/develop/contributing.qmd` | none | Contributor product page, not canonical workflow policy. |
| `docs/develop/flaky-ci-investigation-2026-08-23.md` | `docs/develop/flaky-ci-investigation-2026-08-23.qmd` | text 26>32,40>47,280>288 O | Historical assertion, exception, and verification output; do not rerun native tests as examples. |
| `docs/develop/setup.md` | `docs/develop/setup.qmd` | bash 12>18,20>27,39>47 P | Reader checkout/dependency setup and dashboard dependency installation; network, uv, Node/npm. Runnable-command exemptions need review. |
| `docs/develop/testing.md` | `docs/develop/testing.qmd` | bash 5>11,11>18,17>25,24>33 P | Full/fast/scoped tests and lint in reader checkout; deliberately not run for this inventory. |
| `docs/extend/README.md` | `docs/extend/index.qmd` | python new>33 E | Installed contrib/testing helpers; local `defunkify(double)` assertion, not worker execution or a download. |
| `docs/extend/concepts/adapters-and-executors.md` | `docs/extend/concepts/adapters-and-executors.qmd` | python 16>17 P | Asynchronous response schema with durable state; runtime protocol illustration. |
| `docs/extend/concepts/codecs.md` | `docs/extend/concepts/codecs.qmd` | none | Codec concepts prose. |
| `docs/extend/concepts/extension-model.md` | `docs/extend/concepts/extension-model.qmd` | none | Extension boundary prose. |
| `docs/extend/concepts/plugin-registration.md` | `docs/extend/concepts/plugin-registration.qmd` | none | Discovery prose. |
| `docs/extend/concepts/remote-integrations.md` | `docs/extend/concepts/remote-integrations.qmd` | none | Remote integration prose. |
| `docs/extend/guides/package-integration.md` | `docs/extend/guides/package-integration.qmd` | toml 7>8 P | Reader-defined installed adapter/executor package; metadata should also receive executable validation. |
| `docs/extend/guides/test-integration.md` | `docs/extend/guides/test-integration.qmd` | none | Integration-test guidance prose. |
| `docs/extend/guides/write-adapter.md` | `docs/extend/guides/write-adapter.qmd` | none | Adapter implementation prose. |
| `docs/extend/guides/write-executor.md` | `docs/extend/guides/write-executor.qmd` | none | Executor implementation prose. |
| `docs/extend/guides/write-shared-codec.md` | `docs/extend/guides/write-shared-codec.qmd` | python 6>7 P; toml 25>27 P | Decimal codec template lacks import/package context; installed factory metadata. No canonical downloadable codec selected. |
| `docs/extend/reference/adapter-operations.md` | `docs/extend/reference/adapter-operations.qmd` | python 7>8,21>23,35>38,43>47,65>70 P | Invoke schema, response alternatives, public facade import, cleanup schema, cancel schema respectively. Identities are runtime-owned; import alone is runnable. |
| `docs/extend/reference/built-in-integrations.md` | `docs/extend/reference/built-in-integrations.qmd` | none | Built-in integrations reference prose. |
| `docs/extend/reference/codec-contracts.md` | `docs/extend/reference/codec-contracts.qmd` | python 7>8 P; text 32>34 O | Incomplete `LiteralCodec` protocol and projection graph trace. |
| `docs/extend/reference/executor-lifecycle.md` | `docs/extend/reference/executor-lifecycle.qmd` | none | Lifecycle contract prose. |
| `docs/extend/reference/plugin-api.md` | `docs/extend/reference/plugin-api.qmd` | toml 5>6,33>35,43>46 P | Adapter/executor, codec, dashboard entry-point templates; reader package install context; validation gap. |
| `docs/getting-started.md` | `docs/getting-started.qmd` | bash 5>8 E; python 15>18 E; bash 25>30 E | Visible pip install/init/status, literal DAG, show/log. Fresh page root, package installation access, Local remote. Python now explicitly opens `research-demo`; redundant `python first_dag.py` launch removed. |
| `docs/glossary.md` | `docs/glossary.qmd` | none | Shared definitions. |
| `docs/sharp-bits-and-security.md` | `docs/sharp-bits-and-security.qmd` | python 27>31,52>57,63>69,72>79,77>85,88>97,98>108,106>117,116>128 P; sh 187>200 P | Control-flow counterexample; editable dependency/cache sequence; helper-source injection; NumPy variant; malicious adapter. Detailed dispositions below. |
| `docs/use/README.md` | `docs/use/README.qmd` | none | Researcher landing. |
| `docs/use/concepts/README.md` | `docs/use/concepts/README.qmd` | none | Concepts landing. |
| `docs/use/concepts/artifacts-data-codecs.md` | `docs/use/concepts/artifacts-data-codecs.qmd` | none | Data/artifact/codec prose. |
| `docs/use/concepts/dags-nodes-results.md` | `docs/use/concepts/dags-nodes-results.qmd` | python 7>8 P | Local authoring/named result example; runnable-looking, not an approved exemption merely because it needs a project. |
| `docs/use/concepts/errors-provenance.md` | `docs/use/concepts/errors-provenance.qmd` | none | Retained error/provenance page; not collapsed into errors. |
| `docs/use/concepts/errors.md` | `docs/use/concepts/errors.qmd` | python 11>12,29>31,45>48 P; bash 57>61 P | Existing `dml`/DAG context, deliberate ValueError, failed `err-val` node, runtime-generated error refs. An executable version needs narrow expected-error assertions. |
| `docs/use/concepts/funks-execution-cache.md` | `docs/use/concepts/funks-execution-cache.qmd` | none | Funk/cache concepts prose. |
| `docs/use/concepts/history-remotes.md` | `docs/use/concepts/history-remotes.qmd` | bash 30>31 P | Reader remote/history/dependency state; reference only, not a selected sync workflow. |
| `docs/use/concepts/projects.md` | `docs/use/concepts/projects.qmd` | none | Project concepts prose. |
| `docs/use/concepts/runtimes.md` | `docs/use/concepts/runtimes.qmd` | bash 9>10 P | Reader runtime state/identifiers. |
| `docs/use/guides/README.md` | `docs/use/guides/README.qmd` | none | Guides landing. |
| `docs/use/guides/artifacts.md` | `docs/use/guides/artifacts.qmd` | python 5>6 P | Local project, S3Store/artifact inputs and writable remote. |
| `docs/use/guides/author-a-dag.md` | `docs/use/guides/author-a-dag.qmd` | python 9>10,30>32,50>53,65>69,83>88,103>109 P; mermaid 127>134 D; python 141>149,176>185,200>210,211>222,223>235,264>277 P; text 282>296 O; python 300>315,316>332 P | Local/Script authoring through nested dagclasses; Docker/SSH/Slurm examples need external resources/plugins. Per-block rationale below. |
| `docs/use/guides/custom-codecs.md` | `docs/use/guides/custom-codecs.qmd` | none | Codec guide prose. |
| `docs/use/guides/custom-dag-dashboards.md` | `docs/use/guides/custom-dag-dashboards.qmd` | toml 15>16 P; python 22>24 P | Installed dashboard provider plus tagged selected DAG and dashboard process. Provider stays in external example package, not selected downloads. |
| `docs/use/guides/docker-workloads.md` | `docs/use/guides/docker-workloads.qmd` | python 5>6 P | Docker image/daemon and configured remote; excluded from curated Python collection. |
| `docs/use/guides/inspect-a-completed-dag.md` | `docs/use/guides/inspect-a-completed-dag.qmd` | bash 5>6 P; python 9>11,21>24,38>42,53>58,67>73,78>85 P | Existing `analysis` DAG, then `normalized_data`, `model_output`, `source_data`, `normalize_text`, `normalize_in_docker`; S3 artifacts/scripts and wrapper chain. Needs page-owned seeded graph, not earlier-page state. |
| `docs/use/guides/refresh-cache.md` | `docs/use/guides/refresh-cache.qmd` | bash 5>6 P | Reader cache keys/remote; mutation not selected as an embedded example. |
| `docs/use/guides/remote-execution.md` | `docs/use/guides/remote-execution.qmd` | bash 5>6 P | Reader remote configuration; no SSH example selected. |
| `docs/use/guides/runtime-inspection-cancellation.md` | `docs/use/guides/runtime-inspection-cancellation.qmd` | bash 5>6,13>15 P; python 21>24 P | Existing live runtime, cancellation target, authoring objects; requires owned cancellable work for execution. |
| `docs/use/guides/share-reuse.md` | `docs/use/guides/share-reuse.qmd` | bash 5>6,14>16,22>25,29>33 P | Publication/tagging, fetch/clone, shallow clone/fetch, deepen/unshallow. Retained reference page, explicitly NOT a selected embedded push/pull lesson. |
| `docs/use/guides/temporary-projects.md` | `docs/use/guides/temporary-projects.qmd` | python 5>6 P | Temporary project lifetime/cleanup pattern; needs owned temporary state, not a reason to silently skip runnable code. |
| `docs/use/reference/README.md` | `docs/use/reference/README.qmd` | none | Reference landing. |
| `docs/use/reference/cli.md` | `docs/use/reference/cli.qmd` | bash 46>47 P | CLI synopsis/example with reader project state. |
| `docs/use/reference/configuration.md` | `docs/use/reference/configuration.qmd` | bash 16>17 P | Set remote, add dependency, show config; placeholder S3 buckets and isolated project/config needed. |
| `docs/use/reference/errors.md` | `docs/use/reference/errors.qmd` | none | Error messages/recovery reference. |
| `docs/use/reference/python-authoring.md` | `docs/use/reference/python-authoring.qmd` | python 5>6,24>26,40>43,52>56 P | Import; projection from prior nodes; low-level session/dependency; cache/GC keys and remote. Import is runnable; others need fixtures. |
| `docs/use/reference/runtime-state.md` | `docs/use/reference/runtime-state.qmd` | bash 5>6 P | Reader runtime references. |
| `docs/why-daggerml.md` | `docs/why-daggerml.qmd` | none | Product rationale. |

The mapped pages contain 94 current fences: four E (two Python, two Bash),
84 P, five O, and one D. Of these, 93 correspond to original fences and one is
the new Extend Python cell. No original fenced block is omitted from the ledger.

### Detailed Contextual Python Classifications

These details distinguish actual incomplete/site-specific pseudocode from
potentially runnable lessons currently carrying generic exemption comments.
Line numbers refer to current QMD; all remain P, not selected downloads.

| Page / current fence | Subject and missing context or risk |
| --- | --- |
| `docs/use/guides/author-a-dag.qmd:10` | Literal authoring, named nodes, load; Local is sufficient in principle. Review generic pseudocode exemption. |
| `docs/use/guides/author-a-dag.qmd:32` | Square funk; Script plus corrected mixed indentation before it could be executable. |
| `docs/use/guides/author-a-dag.qmd:53` | Tagged funk fragment; prior `api` import and a caller/Script fixture. |
| `docs/use/guides/author-a-dag.qmd:69` | `extra_objs` helper injection; prior import plus Script/caller. |
| `docs/use/guides/author-a-dag.qmd:88` | External `ghcr.io/acme/forecast:2026.07` image, Docker daemon and resource flags; site-specific. |
| `docs/use/guides/author-a-dag.qmd:109` | SSH host `foo`, credentials env file, non-shipped Slurm plugin, GPU Docker image; genuine hypothetical integration. |
| `docs/use/guides/author-a-dag.qmd:149` | Explicit nested funks; Script/imports/caller needed; related full original is `examples/python/03-dagclass.py`, not this block's download. |
| `docs/use/guides/author-a-dag.qmd:185` | Prepop composition uses prior parse/normalize/summarize definitions and Script. |
| `docs/use/guides/author-a-dag.qmd:210` | Dataset funk with `api.ref` image/flags; scikit-learn/pandas in Docker; related original `examples/python/01-docker_dataset.py`. |
| `docs/use/guides/author-a-dag.qmd:222` | `docker_flags`, `docker_image`, `download`, and `dag` are contextual placeholders. |
| `docs/use/guides/author-a-dag.qmd:235` | DatasetSummary references earlier funks; Script and those definitions needed. |
| `docs/use/guides/author-a-dag.qmd:277` | PreparedSummary requires prior summarize/import; Script/caller needed. |
| `docs/use/guides/author-a-dag.qmd:315` | Incomplete method illustrates an unbound worker node when `ready` is false; do not execute as success. |
| `docs/use/guides/author-a-dag.qmd:332` | MultiDatasetSummary requires DatasetSummary and Script; simplified selected dagclass does not cover full nested behavior. |
| `docs/sharp-bits-and-security.qmd:31` | Same incomplete control-flow counterexample; surrounding dagclass/worker required. |
| `docs/sharp-bits-and-security.qmd:57` | Before-state `foo/bar.py` population variance; external editable-package scenario, not a canonical download. |
| `docs/sharp-bits-and-security.qmd:69` | Funk imports that external package; needs Script and caller. |
| `docs/sharp-bits-and-security.qmd:79` | Call/value comparison needs preceding funk, DAG, and original cached result; not an asserted expected-error test. |
| `docs/sharp-bits-and-security.qmd:85` | After-state sample variance; requires controlled dependency replacement. |
| `docs/sharp-bits-and-security.qmd:97` | Repeated funk is meaningful only with the prior dependency/cache state. |
| `docs/sharp-bits-and-security.qmd:108` | Two cache-identity comparisons; requires that exact history, not an independent page fixture. |
| `docs/sharp-bits-and-security.qmd:117` | Source injection of external `mean` and `variance`; inspectable helper files needed. |
| `docs/sharp-bits-and-security.qmd:128` | Optional NumPy/editable-package variant with injected import; unverified illustrative API spelling, not executable proof. |
| `docs/sharp-bits-and-security.qmd:200` | Intentionally malicious sh adapter; explicit security illustration, never execute in docs fixtures. |

## Added QMD And Build Material

These have no original human-facing docs page to rename. There are 68 authored
QMD files in total: 62 mapped pages, four Examples pages, and two hidden lifecycle
pages. Generated `_build`, `build-staging`, `.quarto`, and `*_files` trees are not
authored pages. There are 97 authored fences across the 68 QMD files (94 mapped
plus three hidden Bash), and four S markers in addition to those fences.

| Current path | Block/inclusion classification | Source / prerequisite disposition |
| --- | --- | --- |
| `docs/examples/index.qmd` | none | New collection landing; prerequisite links to getting-started and configuration; explicitly excludes shell push/pull, Docker, SSH. |
| `docs/examples/script-executor/index.qmd` | S at 9; bash 13 H | `script.py`; hidden check for `RETICULATE_PYTHON`; Script fixture. |
| `docs/examples/dagclass/index.qmd` | S at 9 | `pipeline.py`; Script fixture. |
| `docs/examples/analysis-report/index.qmd` | S at 11 and 13 | `analysis/metrics.py`, then `run_report.py`; Local fixture and preserved module layout. |
| `docs/build-bootstrap.qmd` | bash 11 H | Sources `DOCS_BUILD_LIB`, calls `docs_bootstrap`; Bash coordinator, selected Python, Moto helper. Removed from prepared human-facing render tree. |
| `docs/build-teardown.qmd` | bash 11 H | Sources `DOCS_BUILD_LIB`, calls `docs_teardown`; owned Moto lifecycle state. Removed from prepared human-facing render tree. |
| `docs/build-tooling.md` | none | New build-only tooling note, not a QMD product lesson or an original page. Workflow-governance placement remains for acceptance review. |

Build-generated cells are not extra authored examples: `docs/build.py:expand_sources`
creates output-suppressed Python `runpy.run_path(..., run_name="__main__")` cells
from each S marker and HTML source from the same file. `prepare` injects hidden
R per-page config/root setup and hidden Python `Dml.init` on S pages. The Python
entrypoints remain files so `inspect.getsource` works; executing function text
in isolation would not provide module globals to script workers. No generated
cell is an execution exemption. Generated HTML, figures, bundles, and downloads
are build outputs, not another authored source inventory.

## Original Python Example Decisions

All 13 baseline Python files under `examples/` are listed individually, including
the two outside `examples/python/`. "Selected concept" means a simpler new
lesson represents that subject; it does NOT mean the original file was moved,
copied byte-for-byte, removed, or fully behaviorally covered. All original files
remain outside the embedded collection so existing shell workflows retain their
dependencies. Excluded files have no new canonical download mapping.

| Original Python source | Decision / canonical mapping | Prerequisites and reason |
| --- | --- | --- |
| `examples/python/00-hello_world.py` | Selected concept -> `docs/examples/script-executor/script.py`; original retained | Script, writable remote, DAG-name argument. Original has UUID output/cache reuse; selected deterministic add-one checks 42 instead. Also used by `push_load.sh`, so do not remove it. |
| `examples/python/03-dagclass.py` | Selected concept -> `docs/examples/dagclass/pipeline.py`; original retained | Script and DAG-name argument. Original nested parsing/normalization/member comparison is reduced to AddOffset binding; not full equivalence. |
| `examples/python/00-errors.py` | Excluded; no canonical embedded error example | Script, DAG-name argument; deliberate divide-by-zero causes nonzero exit. Existing shell runner accepts failure status; embedding requires narrow error/type assertions, not permissive rendering. |
| `examples/python/01-docker_dataset.py` | Excluded | Local remote/S3, Docker daemon, repository build context and `examples/dkr-ctx/Dockerfile`, image dependency install/network, pandas/sklearn/Parquet in image. Explicitly expects pandas absent locally. Not a simple independent download. |
| `examples/python/01b-load_fn.py` | Excluded | Prior hello DAG with `hello_fn` and `greeting`, new/prior DAG-name arguments, Script. Part of shell reuse/push-load sequence despite Python extension; stale Docker-oriented docstring is not the dependency source of truth. |
| `examples/python/02-ssh_docker_dataset.py` | Excluded | Prior Docker dataset DAG/image/flags; docker, ssh, sshd, ssh-keygen; local user/key/port/env-file setup and Python subprocess orchestration. Prohibited as an embedded Python shell wrapper. |
| `examples/python/03-load_docker_dataset.py` | Excluded | Prior Docker dataset DAG with `predict_fn` and dataset, Docker/remote artifacts and two name arguments; order-dependent reuse lesson. |
| `examples/python/04-docker-dagclass.py` | Excluded | Prior `01-docker_dataset.py` DAG with image/flags, Docker/pandas image, remote, two names. Selected AddOffset does not cover Docker namespace refs. |
| `examples/python/04-freeze_dag.py` | Excluded (possible future simple lesson, not currently selected) | Local, DAG-name argument; leaves frozen unfinished runtime with named values. No matching canonical docs script exists. |
| `examples/python/wait_fn.py` | Excluded | Script, remote, DAG name, thread pool, delayed nested calls and possible external cancellation. Timing/lifecycle scenario, not selected deterministic lesson. |
| `examples/python/dashboard_secondary_examples.py` | Excluded | Initialized dashboard demo project/remote; creates four low-level literal DAGs. Fixture population, not a selected downloadable lesson. |
| `examples/moto_server_env.py` | Excluded as lesson/download; retained build infrastructure dependency | Moto server, boto3, Python process/network/filesystem lifecycle; called from hidden Bash bootstrap/teardown via `docs/build-lib.sh`. It is not included by any S marker. |
| `examples/dashboard-plugin/src/example_dashboard_plugin/__init__.py` | Excluded as download; retained external integration package | Install `examples/dashboard-plugin/pyproject.toml` entry points in dashboard environment; tagged DAG with nodes; Plotly/Vega-Lite result providers. Related reference page is custom-dag-dashboards, not an embedded example. |

### Selected Canonical Files And Prerequisites

| Canonical Python source | Lesson / download mapping | Checks and prerequisites |
| --- | --- | --- |
| `docs/examples/script-executor/script.py` | `docs/examples/script-executor/index.qmd` S `script.py`; individual `data-download="script.py"` | Script; explicit local/script funk, commit and assert result 42. Worker function is self-contained and needs no module globals. |
| `docs/examples/dagclass/pipeline.py` | `docs/examples/dagclass/index.qmd` S `pipeline.py`; individual `data-download="pipeline.py"` | Script; file-backed AddOffset, `use_default_dml`, `api.run`, load/result assertion 42. Worker accesses compiled `self.offset`, not a Python module global. |
| `docs/examples/analysis-report/analysis/metrics.py` | `docs/examples/analysis-report/index.qmd` S `analysis/metrics.py`; individual same-path download and `analysis-report.zip` | New helper, not a migrated original; standard-library Python only. Source inclusion defines `summarize`; report entrypoint exercises it with nonempty values. |
| `docs/examples/analysis-report/run_report.py` | `docs/examples/analysis-report/index.qmd` S `run_report.py`; individual same-path download and `analysis-report.zip` | New lesson, not a migrated original; Local plus sibling `analysis/metrics.py`. Assert count=3, total=16, largest=8. Bundle must preserve that relative layout. |

Every lesson links `../index.qmd` for prerequisites, which links installation/init
and `remote.root` configuration. Downloads do not configure a reader's endpoint,
credentials, project, or services. Build prerequisites are the pinned Quarto/R/
knitr/rmarkdown/reticulate/xfun toolchain in `docs/build-tooling.md`, a selected
project Python (`DOCS_PYTHON` / `RETICULATE_PYTHON`), Moto server/boto3, and isolated
config/project roots. Selected lessons require neither Docker nor SSH. Moto
capability availability is not proof of CloudWatch provisioning or lifecycle
correctness; task 2.3 remains a separate verification obligation.

## Shell Workflow Exclusion

The embedded Examples selection is exactly script-executor, dagclass, and
analysis-report. There is no embedded shell push/pull lesson, launcher, source
inclusion, or shell download. Specifically, `examples/push_load.sh` stays outside
the collection along with `bash_full_cli_workflow.sh`, `bash_dag_execution.sh`,
`cache_invalidation.sh`, `docker_and_ssh.sh`, `python_examples.sh`,
`run-all-examples.sh`, `setup-dashboard-demo.sh`, and `show-exec-graph.sh`.
`examples/example_helpers.sh`, the Moto helper, Dockerfile, and dashboard-plugin
metadata remain infrastructure/support, not selected lessons. All these shell
paths are under `examples/`; none is migrated by this inventory.

The existing share/reuse and history/reference pages still describe push/fetch/
clone and security notes mention pull. Accounting for those original pages does
not select their shell workflows into `docs/examples/`. This exclusion follows
the change's collection-specific requirement, not a ban on documenting sync.
If "exclude embedded push/pull" were interpreted as removing all sync reference
prose or snippets from dashboard Docs, the current QMD pages would not satisfy
that broader interpretation; no such content removal is claimed here.

## Repository Boundary

The human-facing migration boundary is `docs/`, not every Markdown file in Git.
`README.md` remains the repository entrypoint (four current fences: three Bash
installation/init blocks and one Python DAG; not QMD execution/download claims).
`CONTRIBUTING.md` remains canonical contributor workflow, `c/README.md` remains
C-library contributor/build documentation, and `c/third_party/lmdb/README.md`
remains upstream vendored documentation. None is a deleted original docs page
or selected embedded example. `AGENTS.md`, `DOC_MAP.md`, `openspec/**`,
`.opencode/**`, and `src/daggerml/_core/skills/*.md` are agent/maintainer guidance,
not product-page migrations. No files in these categories are silently counted
as migrated human-facing QMD pages.

## Evidence And Remaining Gaps

Task 4.1's inventory deliverable is satisfied by the complete page/block ledger,
individual Python decisions, prerequisite/source mapping, explicit pseudocode
classifications, and collection-specific shell exclusions. This is inventory
coverage, not evidence that the rest of the change satisfies execution policy.

Recorded lightweight verification results (read-only Node built-ins and Git;
no project imports or generated files):

| Check | Result |
| --- | --- |
| Exact baseline page set vs ledger, plus target existence | PASS: 62/62 |
| Every baseline and mapped-current fence line/language vs ledger | PASS: 93 baseline, 94 mapped-current |
| Current authored QMD set vs mapped and added page rows | PASS: 68/68, 97 total fences |
| Explicit current pseudocode markers on P/O/D ledger entries | PASS: 90/90, split 84 P + 5 O + 1 D |
| Original Python source set vs individual decisions | PASS: 13/13 |
| S marker source set vs canonical Python files and individual download links | PASS: 4/4 |
| Selected sources/lessons exclude push/pull commands, push_load.sh, shell source inclusion | PASS |
| Baseline non-fenced indentation scan | Only list-continuation prose in system-overview, write-adapter, write-executor; no additional indented code blocks |
| Inventory ASCII, trailing whitespace, final newline | PASS |
| `git diff --check` for existing tracked changes | PASS; new untracked inventory checked separately above |

- Lightweight checks compare the exact baseline docs set against the original
  ledger column and the exact current authored QMD set against mapped/additional
  rows, check that every mapped target exists, and compare every fence opening
  line/language and S marker against the ledger. The baseline/current fence
  counts are 93/97, with current split 84 P, five O, one D, four visible E,
  three hidden H, plus four S markers. The mapped subset is 94 fences.
- The baseline Python set is 13 files; the selected source set is exactly four
  files, referenced by four S markers. Every original Python file has a decision;
  every selected source exists and has an individual download link. The report
  lesson also declares its path-preserving bundle. No selected source is a
  shell script, and no selected source/lesson invokes a push/pull workflow.
- Gap for migration/acceptance owners: explicit pseudocode comments cover many
  runnable-looking imports, Local authoring lessons, and contributor commands.
  Isolation prerequisites alone do not establish that code is pseudocode. The
  84 P classifications require semantic review; a source-policy validator
  accepting a marker is not evidence of successful execution.
- Gap for executable-data policy: six TOML, two JSON, and one HTTP sample are
  static P blocks, with no executable validation in their pages. Protocol
  type-shape templates can be genuine pseudocode, but concrete data/config
  examples must be constructed/validated in cells under the design. This
  inventory preserves that distinction rather than counting them as executed.
- No selected lesson currently demonstrates expected-error assertions. The
  original errors script is explicitly excluded; its existence must not be used
  as evidence that task 4.6 or 5.3's expected-error behavior was verified.
- No full builds, Quarto renders, service startup, native imports, or native
  tests were run for this task, because prior runs segfaulted. Runtime isolation,
  actual execution/source/download parity, generated bundle bytes, missing
  prerequisite failure, cleanup, and packaging remain unverified here.
- Only this new maintainer inventory is owned by this task. Existing dirty
  product docs, build scripts, frontend, tests, staged artifacts, and tasks.md
  are preserved without modification.

## Consulted Material

- Required guidance: `AGENTS.md`, `DOC_MAP.md`, `CONTRIBUTING.md`, `README.md`,
  `openspec/README.md`; global docs `docs/index.qmd` and
  `docs/develop/architecture/system-overview.qmd`.
- All change artifacts: `.openspec.yaml`, `proposal.md`, `design.md`, `tasks.md`,
  and the four `specs/*/spec.md` deltas for dashboard-documentation,
  dashboard-revision-navigation, executable-documentation-build, and
  human-facing-project-docs under this change directory; OpenSpec status/apply
  instructions and the openspec-apply-change skill.
- All 62 original docs pages were scanned from Git for block coverage and all
  mapped QMD files for corresponding fences. Current pseudocode annotations
  across Use/Extend/Develop/shared pages were inspected. Focused full reads:
  audience landing pages, getting-started, sharp-bits-and-security,
  use/guides/author-a-dag, inspect-a-completed-dag, share-reuse,
  use/concepts/errors, use/reference/configuration,
  extend/reference/adapter-operations, codec-contracts,
  extend/guides/write-shared-codec, all four Examples QMD pages,
  docs/build-tooling.md, and both lifecycle QMD pages.
- Source inspection: all 13 original Python examples via Git baseline; all four
  selected canonical Python files; `examples/python_examples.sh`,
  `examples/push_load.sh`, `docs/_quarto.yml`, `docs/build-lib.sh`, and
  `docs/build.py` source inclusion/validation/preparation code. Contrib source
  lookup confirmed inspection and injection hooks in `src/daggerml/contrib/api.py`,
  `testing.py`, and `executors/script.py` without importing the native runtime.
