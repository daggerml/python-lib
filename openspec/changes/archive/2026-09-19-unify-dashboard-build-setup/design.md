## Context

See `proposal.md` for motivation and `specs/human-facing-project-docs/spec.md` for the observable contract. `docs/build.sh` currently selects documentation and UI output from fingerprints, bootstraps Quarto/R, invokes the documentation renderer, and runs the Vite production build. CI separately runs `uv sync`, `npm ci`, and `npm test`. Vite writes directly to the packaged static directory with `emptyOutDir`, while documentation is copied there afterward, so a failed build can disturb the last valid package tree.

The build targets Bash on the already supported macOS/Linux and x86_64/ARM combinations. Python, Node/npm, `uv`, Git, `curl`, `tar`, and native build prerequisites remain properties of the host. The ignored `.tools/` tree remains the home for downloaded documentation tools, caches, state, and temporary build products. The repository tracks `dashboard-ui/package-lock.json` but intentionally does not currently track `uv.lock`, so Python synchronization follows project constraints rather than claiming a frozen clean-checkout resolution.

## Goals / Non-Goals

**Goals:**

- Give local, CI, and release callers one repository command that prepares dependencies, verifies the frontend, and produces the complete packaged dashboard.
- Keep setup, verification, output selection, and rebuild policy explicit and independently controllable.
- Preserve a previously valid packaged dashboard until a complete candidate has passed validation.
- Keep help output sufficient to understand prerequisites, defaults, stage interactions, and common invocations without reading the script.

**Non-Goals:**

- Install or version-manage Python, Node/npm, `uv`, compilers, or operating-system packages.
- Run the repository's Python lint, typecheck, or full pytest suite as part of the dashboard build.
- Change published runtime dependencies or require Node, Quarto, or R in an installed DaggerML package.
- Introduce a second public setup wrapper or replace the existing documentation-owned entrypoint.
- Change the repository's current `uv.lock` tracking policy.

## Decisions

### Keep one public coordinator and model setup as stages

`docs/build.sh` remains the public command. Its default stage plan is:

```text
preflight
  ├── python-sync: uv sync --group dev --all-extras
  ├── npm-ci: npm ci in dashboard-ui
  ├── docs-toolchain: existing pinned Quarto/R bootstrap when docs render
  ├── ui-test: npm test
  ├── docs-render: validate, execute, and stage QMD output
  ├── ui-build: TypeScript check and Vite production build
  ├── assemble: combine new and preserved component trees
  └── validate/install: validate candidate, then replace packaged static output
```

The frontend production command continues to provide TypeScript checking through `tsc -b`; a duplicate typecheck stage is not added. Python project tests remain in their existing CI jobs.

Alternative considered: add `docs/setup.sh` above `docs/build.sh`. This would create two plausible entrypoints and weaken the existing single-command contract, so setup is folded into the established coordinator instead.

### Separate stage selection from rebuild policy

The parser accepts multiple options. `--no-python-sync`, `--no-npm-ci`, and `--no-ui-test` disable setup or verification stages. `--no-docs` and `--no-ui` disable output components. The existing automatic fingerprint policy remains the default, while `--full` forces all selected output components. Forced rebuilding never overrides a disabled component.

Setup and verification controls are deliberately independent of output controls. For example, `--no-ui` does not silently disable `npm ci` or `npm test`; the documented documentation-only command names all three disabled stages. This keeps command behavior literal and avoids hidden coupling as stages evolve.

When synchronization is disabled, downstream commands use existing `.venv` or `node_modules` state and report ordinary, actionable failures if it is unusable. When an output component is disabled, assembly requires and preserves its complete packaged counterpart. This makes partial builds safe but means they cannot initialize a clean checkout by themselves.

Alternative considered: retain only mutually exclusive `--docs-only` and `--ui-only` modes. Those modes cannot express setup reuse, test selection, or forced rebuilding independently and lead to an expanding combination matrix.

### Build components separately and install one validated candidate

Documentation and frontend compilation write to separate temporary component roots under `.tools/`, never to `src/daggerml/dashboard/static/`. Assembly creates a sibling candidate tree from newly built selected components and copies unchanged components from the current package tree when necessary. Candidate validation requires the frontend entrypoint/assets and documentation manifest/fragments/assets, parses the manifest, and can run the existing installed-dashboard contract where practical.

Only after candidate validation succeeds does the coordinator swap it into `src/daggerml/dashboard/static/`. The swap keeps the old tree as a temporary sibling backup until the new tree is installed; traps restore the backup if the replacement sequence fails. Build-state fingerprints are committed only after successful installation. Temporary component, candidate, preserved, and backup trees are removed on both success and failure.

Alternative considered: continue allowing Vite to empty the packaged directory and restore documentation afterward. This is simpler but cannot preserve the last valid artifact when compilation or copying fails.

### Make help the canonical command reference

`--help` is handled before preflight or filesystem mutation. It explains:

- host prerequisites and what the script installs;
- default automatic behavior and the complete stage order;
- every disabling and rebuild-policy option;
- existing-state requirements for skipped setup and output stages;
- transactional candidate validation and replacement; and
- the exact comment-labeled command examples required by the delta spec.

`CONTRIBUTING.md` and `docs/build-tooling.md` summarize the command and defer detailed option semantics to `--help`, avoiding three independently maintained option references.

### Keep CI responsible only for host provisioning

GitHub Actions may continue to check out the repository, select Python and Node versions, install the expected `uv` executable, and configure caches. Dashboard and distribution jobs then invoke `bash ./docs/build.sh` without separate repository setup, frontend test, or build commands. This preserves explicit host version policy while making the script the only authority for repository build stages.

## Risks / Trade-offs

- [Always running `uv sync`, `npm ci`, and frontend tests makes the default local command slower] -> Keep automatic output fingerprints and provide the documented `--no-python-sync`, `--no-npm-ci`, and `--no-ui-test` controls for trusted local environments.
- [A skipped setup stage can expose stale dependencies] -> State that skipped stages trust existing state and keep downstream failures explicit rather than attempting partial dependency inference.
- [A partial build on a clean checkout has no counterpart to preserve] -> Validate preserved components before build installation and fail with a component-specific diagnostic.
- [Directory replacement is not a single atomic filesystem operation on every supported platform] -> Use candidate and backup siblings on the same filesystem, minimize the swap window, and restore the prior tree from a trap on failure.
- [Frontend output paths are currently fixed in Vite configuration] -> Make the production output location overridable by the coordinator while retaining the normal development configuration.
- [Python dependency resolution is not frozen in a clean checkout] -> Describe synchronization accurately and leave lockfile policy outside this change rather than implying reproducibility the repository does not provide.
- [Help examples and parser behavior can drift] -> Keep `--help` canonical and review it while manually exercising representative build combinations; helper scripts remain outside the library contract test surface.

## Migration Plan

1. Refactor component output paths and package assembly so docs and Vite build outside the installed static tree and a validated candidate replaces it.
2. Add preflight, Python synchronization, frontend installation, and frontend-test stages; retain existing documentation tool bootstrap and render behavior.
3. Replace mutually exclusive component modes with composable disabling flags while preserving automatic fingerprints and selected-output `--full` behavior.
4. Remove duplicated repository setup and frontend-test commands from dashboard and distribution CI jobs.
5. Update contributor and build-tooling guidance and manually verify help plus clean, incremental, partial, forced, failure, and distribution builds without adding helper-script tests.

Rollback restores the previous script modes, direct Vite output path, and explicit CI setup/test steps together. Generated static output can be rebuilt with the restored workflow; no persisted user data or public runtime API requires migration.
