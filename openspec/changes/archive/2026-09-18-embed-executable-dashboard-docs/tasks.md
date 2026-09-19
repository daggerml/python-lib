## 1. Execution Compatibility Gates

- [x] 1.1 Establish pinned/documented build-only Quarto, R, knitr, rmarkdown, and reticulate tooling with explicit project Python selection; verify a minimal mixed Python/Bash QMD renders and published runtime/optional dependency declarations remain unchanged.
- [x] 1.2 Prove file-backed source execution for representative script-executor and dagclass examples under reticulate; verify successful worker execution and source inspection without core API changes or Python shell wrappers, recording the supported source-inclusion mechanism.
- [x] 1.3 Configure strict Bash execution and error propagation; verify deliberate Python exceptions, Bash failures, and non-final pipeline failures each cause a nonzero top-level build exit with page/cell diagnostics.

## 2. Isolated Documentation Build

- [x] 2.1 Add the knitr QMD project and build entrypoint with evaluation enabled and caching/freezing disabled; verify hidden and output-suppressed cells execute on two consecutive builds.
- [x] 2.2 Implement hidden Bash bootstrap/teardown QMD and a Bash coordinator with failure-preserving cleanup; verify partial setup, rendering failure, and cleanup failure yield nonzero exits and owned resources are released where cleanup is possible.
- [x] 2.3 Provision disposable Moto S3/CloudWatch, per-page DML/config roots, environment inheritance, and required SSH fixtures; verify pages run independently, user configuration is untouched, and missing prerequisites fail instead of skipping.
- [x] 2.4 Add authored-code and execution-policy validation covering QMD/native knitr overrides, static fences, explicit pseudocode, and prohibited Python shell wrappers; verify negative fixtures fail while generated output and non-code diagrams remain valid.
- [x] 2.5 Implement canonical Python file inclusion, file-backed execution, individual downloads, and multi-file bundles under docs/examples; verify displayed/executed/downloaded source equivalence and preserved relative paths.

## 3. Static Content and Dashboard Integration

- [x] 3.1 Produce script-free HTML fragments, static figures/assets, and a navigation/heading/download manifest in clean staging; verify rendered internal links, anchors, asset paths, and absence of fixture credentials or hidden plumbing.
- [x] 3.2 Add global Docs routes and navigation to the persistent React shell using existing dependencies, themes, and responsive patterns; verify no-project access, deep links, anchors, back/forward, and restoration of a prior historical project route.
- [x] 3.3 Serve docs content/downloads in a distinct static namespace and relocate Swagger to /api/docs; verify nested routes, correct download bytes, missing-file 404s, path containment, and unchanged API security behavior in server tests.
- [x] 3.4 Verify docs keyboard navigation, current-page indicators, download interaction, and desktop/mobile light/dark layouts through frontend tests and browser inspection.

## 4. Documentation and Example Migration

- [x] 4.1 Inventory all human-facing pages/code blocks and Python examples, recording selected examples, prerequisites, source-path mapping, and explicit pseudocode classifications; verify the inventory excludes embedded push/pull workflows and accounts for every existing docs page.
- [x] 4.2 Migrate root landing/shared pages and getting started after reading their current docs and relevant CLI/API source; verify installation/setup snippets execute against disposable state, initialization is visibly taught, and navigation preserves onboarding intent.
- [x] 4.3 Migrate Use concepts/guides/reference as an independently owned lane after reading corresponding docs and source; verify every migrated page renders successfully with useful content preserved and unrelated setup hidden.
- [x] 4.4 Migrate Extend concepts/guides/reference as an independently owned lane after reading corresponding contrib/API docs and source; verify executable integration examples and all page links render successfully.
- [x] 4.5 Migrate Develop and architecture material as an independently owned lane after reading corresponding implementation/docs; verify executable snippets, diagrams, and technical depth are preserved without exposing build fixture plumbing.
- [x] 4.6 Migrate selected Python examples into the examples subtree with concise explanations and prerequisite links; verify every selected example executes independently, narrowly checks expected errors, and provides matching downloads without redundant launch commands.
- [x] 4.7 Update maintained repository links, DOC_MAP.md, contributor build instructions, dashboard architecture, and current spec references to migrated source paths; verify no maintained broken links or duplicate migrated source copies remain and non-migrated example workflows still resolve their dependencies.

## 5. CI, Packaging, and Acceptance

- [x] 5.1 Assemble verified documentation after Vite clears its output and use the shared build entrypoint in CI plus wheel/sdist release jobs; verify any docs failure blocks packaging/publication and stale staged output cannot satisfy the gate.
- [x] 5.2 Add distribution checks for nested docs/assets/scripts/bundles and dependency metadata; verify installed wheel and sdist-derived wheel serve Docs without Quarto, R, Node, or fixture services and retain unchanged runtime/optional dependencies.
- [x] 5.3 Run the complete documentation build and cross-layer failure-injection suite; verify setup/Python/Bash/pipeline/render/validation/cleanup failure statuses, hidden execution, missing prerequisites, expected-error assertions, and source/download parity.
- [x] 5.4 Run dashboard frontend tests/build, relevant server/integration tests, and the repository test suite using CONTRIBUTING.md guidance; record results and verify non-migrated examples retain their existing coverage.
- [x] 5.5 Review migrated content and packaged dashboard end to end against all four capability deltas; verify no package dependency changes, no Python-to-Bash wrappers, and no setup leakage in ordinary lessons, recording consulted docs and remaining limitations.
