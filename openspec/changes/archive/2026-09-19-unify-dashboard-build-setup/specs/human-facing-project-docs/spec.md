## MODIFIED Requirements

### Requirement: Documentation build entrypoint SHALL live with documentation
The repository SHALL provide `docs/build.sh` as the single local, CI, and release entrypoint for producing verified executable documentation and packaged dashboard assets. Given host-provided Python, Node/npm, `uv`, source-control and download utilities, and native build prerequisites, the default invocation SHALL synchronize project Python dependencies, install locked frontend dependencies, bootstrap the pinned documentation toolchain, run frontend tests, execute and stage the documentation, compile the frontend, assemble the combined static tree, and validate the packaged result. Dashboard and release automation SHALL invoke this entrypoint rather than duplicate repository dependency-installation, frontend-test, documentation-render, or frontend-build commands. A duplicate root-level dashboard build entrypoint SHALL NOT remain.

#### Scenario: Maintainer performs the complete build
- **WHEN** a maintainer invokes `bash docs/build.sh` on a host with the documented prerequisite tools
- **THEN** the command prepares repository dependencies and produces a verified packaged dashboard containing the current frontend and executable documentation

#### Scenario: CI or release automation builds the dashboard
- **WHEN** automation has provisioned the documented host prerequisites
- **THEN** it invokes `docs/build.sh` as the sole repository command for dashboard dependency setup, verification, rendering, compilation, and packaging

#### Scenario: A host prerequisite is unavailable
- **WHEN** a required host-provided tool is unavailable
- **THEN** the build fails before consuming incomplete generated output and identifies the missing prerequisite

## ADDED Requirements

### Requirement: Dashboard build stages SHALL be composable
The build entrypoint SHALL expose independent options to disable Python dependency synchronization, frontend dependency installation, frontend tests, documentation output, and frontend output. Automatic-versus-forced output rebuilding SHALL remain independent of those stage selections. A disabled setup stage SHALL use existing repository state, a disabled output stage SHALL preserve the corresponding packaged component, and the command SHALL fail clearly when selected downstream work lacks required dependencies or a complete preserved component.

#### Scenario: Contributor performs a fast local rebuild
- **WHEN** a contributor invokes `bash docs/build.sh --no-python-sync --no-npm-ci` with usable dependency environments
- **THEN** the build skips dependency synchronization while retaining verification and both output stages

#### Scenario: Contributor builds documentation only
- **WHEN** a contributor invokes `bash docs/build.sh --no-npm-ci --no-ui-test --no-ui`
- **THEN** the build prepares and renders documentation while preserving the existing packaged frontend

#### Scenario: Contributor builds the frontend only
- **WHEN** a contributor invokes `bash docs/build.sh --no-python-sync --no-docs`
- **THEN** the build installs and verifies frontend dependencies, compiles the frontend, and preserves the existing packaged documentation

#### Scenario: Contributor forces selected outputs
- **WHEN** a contributor combines forced rebuilding with one or more disabled stages
- **THEN** the build forces only the selected output stages and does not re-enable a disabled stage

### Requirement: Dashboard packaging SHALL be transactional
The build SHALL prepare selected documentation and frontend output away from the installed package tree, combine selected output with preserved unselected components, and validate the complete candidate before replacing `src/daggerml/dashboard/static/`. A failed setup, test, render, compilation, assembly, or validation stage SHALL NOT leave a partially replaced packaged dashboard.

#### Scenario: Complete build succeeds
- **WHEN** every selected stage and final validation succeeds
- **THEN** the packaged static tree is replaced with the validated candidate containing both dashboard components

#### Scenario: Selected stage fails
- **WHEN** any selected stage fails
- **THEN** the command exits unsuccessfully and the previously packaged static tree remains intact

#### Scenario: Partial build lacks a preserved component
- **WHEN** documentation or frontend output is disabled and no complete packaged version of that component exists
- **THEN** the command fails before replacing the packaged static tree and identifies the missing preserved component

### Requirement: Build help SHALL document the complete interface
`docs/build.sh --help` SHALL describe host prerequisites, the default complete build, every setup, verification, output-selection, and rebuild-policy option, interactions between disabled and downstream stages, and the generated-output replacement behavior. The help SHALL include comment-labeled examples for the complete clean CI/release build, a fast local rebuild with existing dependency environments, a documentation-only build, and a frontend-only build that preserves packaged documentation.

#### Scenario: Contributor requests build help
- **WHEN** a contributor invokes `bash docs/build.sh --help`
- **THEN** the command exits successfully without installing dependencies or changing generated output and prints the full interface explanation

#### Scenario: Contributor reads build examples
- **WHEN** the help examples are displayed
- **THEN** they include the commands and comment labels `# Complete clean CI/release build` with `bash docs/build.sh`, `# Fast local rebuild with existing dependency environments` with `bash docs/build.sh --no-python-sync --no-npm-ci`, `# Documentation only` with `bash docs/build.sh --no-npm-ci --no-ui-test --no-ui`, and `# Frontend only, preserving packaged docs` with `bash docs/build.sh --no-python-sync --no-docs`
