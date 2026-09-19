# Acceptance Validation Evidence

Validation completed on macOS arm64 with Python 3.14.3 and the pinned build-only
Quarto 1.7.31, R 4.4.3, knitr 1.49, rmarkdown 2.29, reticulate 1.40.0, and xfun
0.49 toolchain. The earlier Linux environment blocker no longer applies.

## Capability Review

| Capability delta | Result |
| --- | --- |
| `dashboard-documentation` | PASS: packaged Docs routes, fragments, nested assets, Python downloads, ZIP bundles, missing-file behavior, keyboard operation, and responsive light/dark layouts were verified. |
| `dashboard-revision-navigation` | PASS: Home and Docs remain global, Docs needs no selected project, current-page state is exposed, and browser history restores historical project scope. |
| `executable-documentation-build` | PASS: the complete clean build executes all 60 project-discovered pages plus README pages, validates links/source parity, stages only verified output, and fails closed for all injected failure classes. |
| `human-facing-project-docs` | PASS: migrated QMD navigation preserves the Use, Extend, Develop, getting-started, and Examples paths; the inventory accounts for all migrated pages and excludes push/pull workflows from embedded examples. |

The `pyproject.toml` dependency and optional-dependency declarations are
unchanged. Static inspection found no Python-to-Bash wrappers in authored QMD or
canonical example source. Staged fragments contain no `<script>` elements and no
hidden fixture paths, credentials, Moto commands, or build-work variables. The
only staged `DML_CONFIG_HOME` occurrence is the configuration reference page,
where environment configuration is the lesson subject.

## Verification Results

| Check | Result |
| --- | --- |
| Complete `bash docs/build.sh` | PASS: clean render, staging validation, and teardown |
| Real coordinator failure injection | PASS: 10/10 setup, Python, Bash, pipeline, render, validation, cleanup, expected-error, missing-Moto, and repeated-success cases |
| Fast docs build-policy/source tests | PASS: 36 passed |
| Distribution acceptance | PASS: direct wheel and sdist-derived wheel; nested fragments/assets/scripts/bundles, unchanged metadata, and runtime serving without build tools/processes/services |
| Dashboard frontend tests | PASS: 53 passed |
| Dashboard production build | PASS: TypeScript and Vite build |
| Browser inspection | PASS: 1440x1000 and 390x844, dark and light; keyboard Enter/Space, focus-visible, current-page state, exact script/ZIP downloads, no overflow, and no page errors |
| Focused dashboard server contracts | PASS: 12 passed |
| Maintained examples | PASS: all examples, including Docker/SSH and non-embedded push/load workflows |
| Full repository suite | PASS: 807 passed, 4 expected xfails, 3 warnings |

## Fixes From Acceptance

- Normalize Quarto project, render, and staging roots before containment and
  relative-path checks so macOS `/var` to `/private/var` aliases do not produce
  false failures.
- Make the browser harness deterministic about its initial dark theme and serve
  download fixtures through Vite so Chromium verifies real response bytes.
- Correct the distribution inventory to require canonical scripts under the
  download namespace, then run the installed-package checker against both wheel
  origins with declared dashboard dependencies.
- Exclude canonical/generated docs examples from repository doctest discovery;
  their file-backed execution remains covered by the executable docs suite.

## Remaining Limitations

- Browser inspection uses an external Playwright module and Chromium executable
  supplied through environment variables; it is recorded acceptance evidence,
  not an npm dependency or CI job.
- The complete documentation build intentionally requires the pinned external R
  and Quarto toolchain. Installed distributions do not require those tools.
- The repository suite retains four unrelated, explicitly expected flaky-CI
  reproducer xfails and reports three Python fork deprecation warnings.

## Consulted Docs

- `AGENTS.md`, `DOC_MAP.md`, `CONTRIBUTING.md`, `README.md`, and
  `openspec/README.md`.
- This change's `proposal.md`, `design.md`, `tasks.md`, and all four capability
  delta specs.
- `docs/index.qmd`, `docs/build-tooling.md`, `docs/develop/testing.qmd`, and
  `docs/sharp-bits-and-security.qmd`.
- `docs/develop/architecture/system-overview.qmd`, `dashboard.qmd`,
  `execution-and-runtime-state.qmd`, and `remotes-and-sync.qmd`.
- `maintainer-inventory.md` and the migrated audience/example source set through
  the complete executable build and staged-output audit.
