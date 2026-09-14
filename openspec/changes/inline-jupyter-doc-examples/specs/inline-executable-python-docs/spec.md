## Purpose

Lets executable documentation present ordinary inline Python definitions while preserving direct shell execution and fail-closed build verification.

## ADDED Requirements

### Requirement: Executable pages SHALL use an engine suited to their authored language
Python-only lessons that require Python definition source SHALL execute in a source-aware Python kernel. Pages that demonstrate shell commands SHALL execute those commands as native Bash cells. The documentation build SHALL support both page kinds in one dependency-ordered build.

#### Scenario: Python lesson defines a decorated function inline
- **WHEN** a Python-only lesson defines and uses a decorated function in an executable cell
- **THEN** the function source is available to the library and the displayed code is the code that executes

#### Scenario: Shell lesson demonstrates a command
- **WHEN** a lesson contains an authored shell command
- **THEN** the command executes directly as a Bash cell rather than through a Python shell escape

### Requirement: Page fixtures SHALL remain isolated across execution engines
Every executable page SHALL receive its declared project home, working directory, and fixture environment before authored code runs, independent of its execution engine. Page dependencies SHALL execute in topological order and share only the explicitly declared filesystem and service artifacts.

#### Scenario: Dependent Python page executes
- **WHEN** a Python-kernel page declares a project home and a dependency on an earlier page
- **THEN** it starts in that project home with isolated environment configuration after its dependency succeeds

### Requirement: Build validation SHALL remain fail-closed for every engine
The documentation build SHALL reject disabled execution, undeclared static executable examples, incompatible language cells, and unexpected cell failures for all supported engines.

#### Scenario: Jupyter page contains an incompatible executable cell
- **WHEN** a Python-kernel page contains a native Bash or R cell
- **THEN** validation fails with an actionable page diagnostic

#### Scenario: Inline Python example fails
- **WHEN** an inline Python cell raises an unexpected exception
- **THEN** the top-level documentation build fails and identifies the page or cell

### Requirement: Inline course lessons SHALL not require duplicate source files
An inline course lesson SHALL execute its displayed Python cells directly. Canonical file injection remains permitted for downloadable standalone examples, but SHALL NOT be required merely to make inline function source inspectable.

#### Scenario: Reader views a function lesson
- **WHEN** the lesson defines a function for immediate use in the narrative
- **THEN** the definition appears in an ordinary executable Python cell without hidden source injection from a duplicate file
