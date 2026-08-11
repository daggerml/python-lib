## Purpose

Makes each funkified Python script execute with the identity, source metadata, and logging behavior of an ordinary imported module while remaining isolated from the authoring process.

## ADDED Requirements

### Requirement: Script source executes as a live module
The script worker SHALL execute generated funkified source as a top-level module named `_daggerml_live`, SHALL register that module in `sys.modules` before executing its source, and SHALL invoke the configured funk from the executed module.

#### Scenario: Funk observes its module identity
- **WHEN** a funkified script reads its function's `__module__` or the source module's `__name__`
- **THEN** the observed value is `_daggerml_live`

#### Scenario: Script imports its live module
- **WHEN** executing script source imports `_daggerml_live`
- **THEN** Python resolves the in-progress module from `sys.modules`

### Requirement: Live module exposes ordinary import metadata
The live module SHALL expose `__name__`, `__file__`, `__package__`, `__loader__`, and `__spec__` values consistent with importing a top-level Python source module, and executed frames SHALL identify the materialized live-module source file.

#### Scenario: Script inspects import metadata
- **WHEN** executing script source inspects its standard module metadata
- **THEN** the metadata identifies `_daggerml_live` as a top-level source module with a Python source file and loader

#### Scenario: Script execution raises an exception
- **WHEN** the live module or invoked funk raises an exception
- **THEN** the returned worker failure traceback identifies the live-module source file and relevant source line

#### Scenario: Module source execution fails
- **WHEN** live-module source raises before module initialization completes
- **THEN** the worker removes the partially initialized `_daggerml_live` module from `sys.modules` before reporting the failure

### Requirement: Live module logging is captured through stderr
The worker SHALL provide the live module with a logger named `_daggerml_live`, configured at DEBUG level with a stream handler targeting worker stderr. The logger SHALL NOT duplicate records through ancestor handlers and SHALL NOT alter logger levels or handlers outside the `_daggerml_live` hierarchy.

#### Scenario: Script uses the injected logger
- **WHEN** the script emits a DEBUG record through the injected `logger`
- **THEN** the formatted record is written once to worker stderr

#### Scenario: Script obtains its logger by module name
- **WHEN** the script calls `logging.getLogger(__name__)`
- **THEN** it receives the configured `_daggerml_live` logger and DEBUG records are captured

#### Scenario: Dependency logger emits a debug record
- **WHEN** a third-party dependency emits a DEBUG record through another logger hierarchy
- **THEN** live-module logger configuration leaves that dependency logger's level, handlers, and propagation behavior unchanged

### Requirement: Existing script function invocation remains stable
Live-module execution SHALL preserve generated source contents, configured function selection, DAG arguments, and result commit behavior.

#### Scenario: Existing funk succeeds
- **WHEN** an existing valid funkified script executes through the live module
- **THEN** the worker invokes the same configured function with the same DAG and argument nodes and returns the same successful DAG result contract

#### Scenario: Authoring module has unrelated globals
- **WHEN** a funkified function references a name that is absent from generated source and was not explicitly injected
- **THEN** execution fails rather than transferring globals from the authoring process
