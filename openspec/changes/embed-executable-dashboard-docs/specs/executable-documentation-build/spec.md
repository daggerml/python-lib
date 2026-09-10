## Purpose

Ensure published documentation and example downloads are verified through isolated build-time execution with failures observable by CI and release automation.

## ADDED Requirements

### Requirement: Documentation builds SHALL execute all authored runnable code
Every non-pseudocode code block in the documentation build SHALL execute on every build, including hidden code and code with suppressed output. Builds SHALL reject execution-disabling configuration and non-executable authored code fences unless explicitly marked pseudocode. Generated output and non-code diagrams SHALL not be treated as runnable source. Data/config examples SHALL be exercised or validated through executable cells.

#### Scenario: Output is suppressed
- **WHEN** a cell hides its output or its entire presentation
- **THEN** its code still executes and contributes to build success or failure

#### Scenario: Execution is bypassed
- **WHEN** a page or cell disables evaluation, enables execution reuse, or contains an unmarked static code example
- **THEN** the documentation build fails rather than publishing unchecked code

### Requirement: Shell commands SHALL execute directly as Bash cells
Authored shell commands SHALL run in executable Bash blocks in QMD, not through Python subprocess APIs, shell magics, or any other Python library used to run shell commands.

#### Scenario: A lesson demonstrates a CLI command
- **WHEN** the documentation build reaches its executable Bash block
- **THEN** Bash executes the command directly and its failure is subject to the build failure contract

### Requirement: Unexpected failures SHALL fail the top-level build
Setup, Python, Bash, pipeline, rendering, validation, and cleanup failures SHALL produce a nonzero top-level build exit code with actionable diagnostics. Expected-error demonstrations SHALL assert the specific expected failure and fail if it does not occur or differs. Cleanup SHALL run after partial setup or execution failure and SHALL not mask the original failure. Failed builds SHALL not publish stale or partial documentation as verified artifacts.

#### Scenario: Python raises an unexpected exception
- **WHEN** an executed Python cell raises unexpectedly
- **THEN** the top-level build exits nonzero and identifies the failing page/cell

#### Scenario: Bash command or pipeline fails
- **WHEN** a Bash command fails, including a non-final pipeline command under strict execution
- **THEN** the top-level build exits nonzero even if later shell operations could otherwise succeed

#### Scenario: Expected error does not occur
- **WHEN** an expected-error example completes without the asserted error
- **THEN** the build fails

#### Scenario: Partial setup fails
- **WHEN** fixture setup fails after acquiring resources
- **THEN** owned resources are cleaned up and the build preserves a nonzero exit status

#### Scenario: Cleanup fails after successful execution
- **WHEN** rendering succeeds but fixture cleanup fails
- **THEN** the top-level build exits nonzero and reports the cleanup failure

### Requirement: Execution fixtures SHALL be isolated and hidden
Build execution SHALL use disposable configuration, project databases, and service resources without touching user state or production services. Required endpoint and DML environment configuration SHALL be supplied behind the scenes. Lessons not about setup/environment SHALL not expose fixture plumbing. Getting started SHALL demonstrate initialization explicitly. Included examples with unavailable prerequisites SHALL fail rather than skip.

#### Scenario: Ordinary example runs
- **WHEN** a Python or CLI example needs DML state or a Moto/SSH fixture
- **THEN** hidden setup provides isolated resources and ordinary API/CLI calls discover their configuration automatically
- **AND** fixture environment variables and setup commands do not appear in the lesson

#### Scenario: Getting started initializes a project
- **WHEN** the getting-started initialization command executes
- **THEN** it initializes a disposable project not already initialized by hidden setup

### Requirement: Downloadable source SHALL match executed examples
Embedded examples SHALL publish download links for their canonical Python script(s). Displayed source, executed source, and downloads SHALL derive from the same canonical files. Multi-file downloads SHALL preserve required relative paths. Prerequisites SHALL be discoverable without embedding build fixture plumbing in unrelated lessons. Redundant Bash launch commands need not be displayed; displayed commands SHALL execute.

#### Scenario: Reader downloads an example
- **WHEN** the reader downloads its Python script or multi-file bundle
- **THEN** its source matches the code verified by the documentation build and its prerequisite guidance is reachable

### Requirement: Documentation tooling SHALL remain build-only
The change SHALL leave published package runtime and optional dependencies unchanged. CI and release packaging SHALL gate on successful documentation execution and rendering. Installed distributions SHALL contain verified static docs and downloads without requiring documentation tools or services at runtime.

#### Scenario: Distribution runs without build tools
- **WHEN** a built distribution is installed without Quarto, R, Node, or fixture services
- **THEN** the dashboard serves its packaged docs and downloads without executing their source
