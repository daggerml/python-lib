## 1. Coverage Ownership and Fixtures

- [x] 1.1 Add a maintained capability-to-lifecycle matrix covering every public API, CLI, dashboard, contrib, storage, and distribution surface, with deterministic and external acceptance ownership.
- [x] 1.2 Add composable fixtures for local projects, publisher/clone Moto remotes, dependency satellites, shallow merge histories, real remote runtime/cache, and installed distributions.
- [x] 1.3 Document deterministic slow-test, Docker-capable, serial-process, and credential-gated external-acceptance marker policy in contributor guidance.

## 2. Repository and History Lifecycles

- [x] 2.1 Add public multi-repository collaboration scenarios for clone/read/write/push, observer verification, status transitions, divergence, non-fast-forward rejection, merge or rebase recovery, and publication.
- [x] 2.2 Add public shallow-clone scenarios for branch, tag, and exact revisions; tip DAG usability; deepening/unshallowing; and a published merge tip with every parent materialized. Retain strict expected failures for public tag publication and shallow-tip inspection pending a separate product-behavior change.
- [x] 2.3 Add dependency satellite scenarios for add/fetch/list/revision inspection/diff/DAG checkout and isolation from the primary remote and execution namespace.
- [x] 2.4 Add public local and remote garbage-collection scenarios proving branch, tag, shallow, and runtime roots survive while publicly orphaned state is collected. Keep the shallow-tip inspection assertion as a strict expected failure pending a separate product-behavior change.

## 3. Authoring, Runtime, and Extension Lifecycles

- [x] 3.1 Extend public API lifecycle coverage for authoring, imports, projections, persisted errors, freeze/resume, remote execution results, and the `Dag.cancel` wrapper.
- [x] 3.2 Add real runtime/cache scenarios for record and graph inspection, reuse, concurrent convergence, cancellation, invalidation/rerun, cleanup retry, and remote-GC retention of the current execution.
- [x] 3.3 Add S3 artifact and optional dataframe-codec round trips, including installed codec-plugin discovery and worker consumption where supported.
- [x] 3.4 Add process-boundary local-adapter and script-executor scenarios for wire protocol, cleanup retry, cancellation, process-group termination, and scratch cleanup.
- [x] 3.5 Add Docker-capable lifecycle scenarios for poll progression, container failure, cleanup retry, cancellation, image-tar loading, and nested dagclass execution.

## 4. Installed Product Lifecycles

- [x] 4.1 Add an isolated-wheel CLI scenario covering installed entry-point discovery, project initialization, authoring/inspection, remote configuration, push/fetch, and shallow clone through real subprocesses.
- [x] 4.2 Add an installed dashboard launcher scenario covering Uvicorn readiness, static UI serving, authenticated API access, registered-project discovery, and graceful shutdown over real HTTP.
- [x] 4.3 Add an installed dashboard-plugin scenario covering natural entry-point discovery, compatible DAG selection, render/cache/refresh behavior, and public endpoint output.
- [x] 4.4 Decide whether the executable-docs build can expose a stable final workspace and, if so, add a compact acceptance assertion rather than duplicating the complete course.

## 5. External Acceptance and Verification

- [x] 5.1 Add credential-gated SSH acceptance scaffolding and runbook for remote adapter execution, polling, cleanup, cancellation, environment files, and remote S3 access.
- [x] 5.2 Add credential-gated AWS acceptance scaffolding and runbook for real S3, Lambda, and Batch execution, logs, cleanup, cancellation, and resource teardown.
- [x] 5.3 Configure CI to run deterministic slow lifecycle coverage in its supported environments and report Docker and external acceptance suites separately.
- [x] 5.4 Audit every matrix entry for a passing owner or an explicitly deferred/unverified claim; run fast, full deterministic, and Docker-capable suites; document unavailable external acceptance without marking it covered. Public tag publication and shallow-tip inspection remain deferred product-behavior gaps, not passing owners.
