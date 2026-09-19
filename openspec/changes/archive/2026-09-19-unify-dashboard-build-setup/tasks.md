## 1. Build Script Verification

- [x] 1.1 Review `--help` output and manually exercise multi-option, automatic, forced, documentation-only, frontend-only, and prerequisite-failure behavior without adding helper-script tests to the library contract suite.
- [x] 1.2 Manually verify transactional candidate installation and preservation behavior for failed and partial builds.

## 2. Composable Setup And Verification

- [x] 2.1 Refactor `docs/build.sh` argument handling to accept composable `--no-python-sync`, `--no-npm-ci`, `--no-ui-test`, `--no-docs`, and `--no-ui` flags while retaining default automatic fingerprints and selected-output `--full` behavior.
- [x] 2.2 Expand `docs/build.sh --help` with prerequisites, stage behavior, every option, skip-state requirements, transactional replacement behavior, and the exact labeled examples from the spec.
- [x] 2.3 Add preflight checks for host-owned tools and clear failures before generated output is consumed or modified.
- [x] 2.4 Add default Python synchronization with `uv sync --group dev --all-extras`, frontend installation with `npm ci`, and frontend verification with `npm test`, each controlled by its corresponding flag.
- [x] 2.5 Preserve the existing pinned, isolated Quarto/R bootstrap and run it only when the selected documentation output requires rendering.

## 3. Transactional Dashboard Packaging

- [x] 3.1 Make documentation staging and Vite production output paths coordinator-selectable so neither component build writes directly to the installed static tree.
- [x] 3.2 Assemble a complete candidate tree from newly built selected components and validated preserved components, covering frontend entrypoint/assets and documentation manifest/fragments/assets.
- [x] 3.3 Validate the combined candidate before installation, then swap it into `src/daggerml/dashboard/static/` with same-filesystem backup and failure restoration.
- [x] 3.4 Clean temporary component, candidate, and backup trees on success and failure, and write component fingerprints only after successful candidate installation.

## 4. Automation And Maintainer Guidance

- [x] 4.1 Remove duplicated `uv sync`, `npm ci`, and `npm test` repository-build steps from dashboard, wheel, and source-distribution jobs while retaining host Python, Node, `uv`, and cache provisioning followed by `bash ./docs/build.sh`.
- [x] 4.2 Update `CONTRIBUTING.md` and `docs/build-tooling.md` with the host/setup ownership boundary, default one-command build, composable-stage summary, transactional output behavior, and direction to `--help` for the complete interface.
- [x] 4.3 Update `DOC_MAP.md` or co-located build orientation only if needed to keep the affected build paths and authority references accurate.

## 5. Verification

- [x] 5.1 Run frontend tests and a production frontend build through the unified script, including fast-local, documentation-only, frontend-only, automatic, and forced representative combinations.
- [x] 5.2 Run executable-document logic and distribution contract tests, including packaged wheel/sdist checks.
- [x] 5.3 Run the required repository typecheck, lint-fix, and non-slow test commands and confirm generated dashboard assets remain valid after successful and failed builds.
