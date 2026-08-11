## Why

The script executor currently evaluates generated source in an anonymous dictionary, so funkified code does not receive the module identity, import metadata, traceback filenames, or logger behavior of ordinary imported Python code. Executing each script as an isolated live module makes user code behave like normal Python while keeping DaggerML's execution machinery out of its observable namespace.

## What Changes

- Execute generated script source as a temporary imported module named `_daggerml_live` instead of through an anonymous `exec` namespace.
- Give the live module standard import metadata and register it in `sys.modules` while the worker runs.
- Run the module from a real temporary Python source file so tracebacks, frame metadata, and logging identify the script source normally.
- Provide `_daggerml_live` with a dedicated DEBUG logger and stderr handler; the supervisor continues capturing worker stderr through its existing subprocess pipe.
- Preserve the current function lookup, DAG invocation, result, failure, cache, and supervisor contracts.

## Capabilities

### New Capabilities

- `script-module-execution`: Defines module identity, import semantics, source metadata, and logging behavior for funkified scripts executed by the script worker.

### Modified Capabilities

None.

## Impact

- Affected code: `src/daggerml/contrib/executors/script.py` and script-executor contract/integration tests.
- Affected documentation: built-in script integration and Python authoring guidance.
- Supervisor process management, CloudWatch streaming, runnable serialization, and public `funkify` arguments remain unchanged.
- No new runtime dependency is required; the implementation uses Python's standard import machinery and logging package.
