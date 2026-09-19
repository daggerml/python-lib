## Why

`docs/build.sh` is the documented dashboard build entrypoint, but a clean build still depends on CI or the contributor separately running Python and frontend dependency setup and frontend tests. This leaks repository build knowledge into automation and means the advertised one-command build is not actually sufficient on a prepared host.

## What Changes

- Make `docs/build.sh` own Python dependency synchronization, frontend dependency installation, the pinned Quarto/R bootstrap, frontend tests, executable documentation rendering, frontend compilation, packaged-output assembly, and final validation.
- Keep Python, Node/npm, `uv`, source-control/network utilities, and native build prerequisites as host-provided tools.
- Make the default invocation perform the complete verified dashboard build used by CI and releases.
- Add composable flags that disable individual setup, verification, documentation, and frontend stages while preserving clear dependency behavior; retain forced-versus-automatic rebuild selection as a separate concern.
- Expand `--help` to explain every stage and option and include the agreed, comment-labeled examples for complete, fast local, documentation-only, and frontend-only builds.
- Assemble documentation and frontend output in temporary locations, validate the combined dashboard, and replace the packaged static tree only after success.
- Reduce dashboard and release CI jobs to environment provisioning followed by the single build-script invocation; remove duplicated `uv sync`, `npm ci`, and frontend-test commands from those jobs.

## Capabilities

### New Capabilities

None.

### Modified Capabilities

- `human-facing-project-docs`: Strengthen the documentation-owned build entrypoint contract to include repository dependency setup, verification, composable stage selection, transactional packaging, complete help, and use as the sole dashboard build command in automation.

## Impact

- Affected tooling: `docs/build.sh`, supporting documentation build scripts, Vite output configuration, and dashboard build-state handling.
- Affected automation: dashboard CI and release distribution jobs in `.github/workflows/ci.yml`.
- Affected documentation and verification: `docs/build-tooling.md`, `CONTRIBUTING.md`, manual build-mode checks, and existing packaging/distribution contracts. Helper scripts are not added to the library contract test surface.
- Host prerequisites remain Python, Node/npm, `uv`, Git, download/archive utilities, and native build tools; published Python runtime dependencies are unchanged.
