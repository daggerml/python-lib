## Context

See `proposal.md` for motivation. Existing integration tests already cover Moto-backed clone bootstrap, linear shallow sync, remote transport/GC internals, real script-funk execution, and the executable Docker documentation course. Most other coverage is either local, mock-backed, or narrowly focused on one method. The suite needs a durable definition of complete integration coverage without replacing precise contract tests with one monolithic scenario.

## Goals / Non-Goals

**Goals:**

- Assign every public capability an integration owner: deterministic lifecycle scenario, external acceptance scenario, or an explicit rationale for contract-only coverage.
- Exercise public APIs and installed commands across real persistence, remote transport, and process boundaries.
- Reuse small, composable fixture worlds so each failing scenario identifies one lifecycle clearly.
- Keep deterministic local/Moto/Docker tests in the normal slow suite and isolate credentialed infrastructure acceptance.

**Non-Goals:**

- Collapse the suite into a single all-functions test.
- Duplicate protocol corruption, invalid-shape, exact-error, or race-injection contracts already owned by focused tests.
- Treat Moto as proof of AWS Batch, Lambda, or production S3 behavior.
- Change repository, runtime, CLI, dashboard, or executor behavior.

## Decisions

### Maintain a capability-to-lifecycle matrix

Add a maintained test-coverage document or structured inventory that maps every public surface to its focused-contract owner, deterministic lifecycle owner, and external-acceptance owner when needed. A capability is covered only when its owner passes; an explicit entry alone establishes ownership, not verification. Contract-only exceptions must say why an end-to-end lifecycle has no value. Known product-behavior failures may be deferred only when named in the matrix, retained as expected-failure tests, and excluded from passing coverage.

Alternatives considered:

- Infer completeness from code coverage. Rejected because execution coverage cannot establish that independently initialized repositories, workers, and installed commands compose correctly.
- Add tests opportunistically. Rejected because it cannot establish or preserve the requested all-functionality boundary.

### Use six composable deterministic fixture worlds

1. **Local project:** real `Dml.init`, LMDB, reopen, public authoring and repository operations.
2. **Publisher/clone remote:** Moto server plus producer, consumer, and observer projects for public remote synchronization.
3. **Dependency satellite:** producer endpoint separate from consumer project remote for import-only behavior.
4. **Shallow merge history:** published merge graph to test depth across every parent.
5. **Remote runtime/cache:** Moto plus real production script/local adapter path, execution state, cache, cancellation, invalidation, and remote GC.
6. **Installed distribution:** non-editable wheel virtual environment, real console scripts, Moto endpoint, subprocesses, and optionally an installed dashboard plugin.

Fixture worlds expose public setup steps and create unique remote roots. They do not use `NoopExecutionState` or private remote mutation for tests that claim production lifecycle coverage.

Alternatives considered:

- One universal preloaded fixture. Rejected because hidden state makes tests hard to understand, isolate, and diagnose.
- Build each world independently in every test. Rejected because it repeats fragile infrastructure setup and obscures scenario intent.

### Organize deterministic scenarios by boundary

The slow suite will own these scenario families:

| Boundary | Required scenarios |
| --- | --- |
| Repository collaboration | initialize/reopen/publish; clone/read/write/push/observer verification; divergence with FF-only rejection then merge or rebase and successful publication; branch/upstream status transitions |
| Shallow history | branch/tag/exact clone usability; deepen/unshallow; merge-tip depth across all parents; shallow publication rules |
| Dependencies | add/fetch/list/resolve/show/log/diff/check out a dependency DAG; prove dependency isolation from the project remote and runtime/cache |
| Tree/history/GC | remote DAG checkout and inspection; merge/rebase/revert collaboration; public local and remote GC preserves live branch/tag/runtime roots and removes public-orphaned state |
| Public authoring | create/commit/load/import/projection/freeze/resume/persisted-error flow; authoring plus real remotely executed result; `Dag.cancel` wrapper |
| Runtime/cache | real execution record/graph; cache reuse; concurrent convergence; cancellation; invalidation/rerun; cleanup retry; remote GC of stale attempts |
| Storage/codecs | S3 artifact and dataframe-codec round trips; installed codec plugin discovery and worker consumption |
| Adapters/executors | local-adapter process protocol; script cleanup/cancel process behavior; Docker poll/failure/cleanup/cancel; dagclass nested/Docker lifecycle |
| Installed CLI | wheel-installed `dml` initializes, authors, inspects, configures, pushes, fetches, clones shallowly, and discovers built-in entry points via subprocesses |
| Dashboard | installed `dml-dashboard` launches over a real socket, serves UI/API, honors auth/config/project registration, and discovers/renders installed dashboard plugins |
| Documentation/distribution | retain executable course build as its existing broad Docker acceptance owner; add compact final-state acceptance only if its workspace can be inspected without duplicating the whole build |

Existing real lifecycle tests remain and are extended where they already own a boundary; only missing workflows require new modules.

### Defer product-behavior gaps exposed by the suite

The public tag publication scenario cannot pass because `push` publishes branches but has no public remote-tag publication path. Two shallow-tip `Dml.show` scenarios cannot pass because inspection traverses a missing parent (including a depth-one merge tip). Keep all three assertions as strict expected failures, identify them as unverified in the matrix, and address public tag publication and shallow-tip inspection in a separate product-behavior change. The passing branch/exact-revision, deepen/unshallow, GC, and merge-parent scenarios remain owners of their respective verified claims. Do not treat these expected failures or unavailable credential-gated acceptance tests as successful lifecycle coverage.

### Separate external acceptance

SSH, AWS Lambda, AWS Batch, and real AWS S3 scenarios are externally provisioned acceptance tests. They require explicit markers such as `external` in addition to `slow`, are skipped without declared credentials/configuration, never run in the default deterministic suite, and document the required infrastructure and cleanup guarantees.

Alternatives considered:

- Simulate them with Moto/fakes. Rejected because that would label unsupported emulation as an integration guarantee.
- Run them unconditionally in CI. Rejected because credentials, cost, account state, and infrastructure availability make them unsuitable for normal PR tests.

## Risks / Trade-offs

- [Slow suite becomes expensive] -> Keep fixture worlds narrow, share session Moto infrastructure, select scenarios by boundary, and retain fast contracts for matrices.
- [Subprocess/Docker tests flake] -> Use readiness polling, unique roots, explicit timeouts, process/container cleanup, and serial execution where required.
- [Integration duplicates contracts] -> The coverage matrix identifies the one lifecycle claim each test owns; invalid and injected-race cases remain contracts.
- [External tests drift] -> Require environment validation, explicit skips, cleanup, and a documented runbook; report them separately from deterministic CI.
- [Expected failures inflate coverage claims] -> Report deferred public tag publication and shallow-tip inspection separately from passing owners and keep their strict expected-failure assertions until the product fixes land.
- [Installed suites test the checkout accidentally] -> Use non-editable environments, `python -I`, absolute console-script paths, and isolated config/project homes.

## Migration Plan

1. Land the naming migration first so new suites use the simplified behavior-first convention.
2. Add the coverage matrix and shared fixture-world helpers.
3. Extend repository, dependency, shallow-history, runtime/cache, GC, and authoring lifecycle tests against public APIs.
4. Add subprocess-installed CLI and dashboard/plugin scenarios.
5. Add Docker lifecycle coverage in the Docker-capable CI job and retain the existing executable-docs workflow.
6. Add gated external acceptance scaffolding and runbooks for SSH and AWS services.
7. Make CI run deterministic slow coverage and report external acceptance separately.

Rollback removes only test infrastructure and CI selection changes; no persisted product data or protocol migration is involved.
