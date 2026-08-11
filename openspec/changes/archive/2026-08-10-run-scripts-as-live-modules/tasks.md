## 1. Live Module Contract Tests

- [x] 1.1 Extend script-worker contract coverage for `_daggerml_live` module identity, standard import metadata, self-import through `sys.modules`, and unchanged successful function invocation.
- [x] 1.2 Add contract coverage for `_daggerml_live.py` traceback filenames and removal of a partially initialized module after source execution fails.
- [x] 1.3 Add logging coverage proving injected and `logging.getLogger(__name__)` DEBUG records each reach stderr once while unrelated logger levels, handlers, and propagation remain unchanged.

## 2. Script Worker Implementation

- [x] 2.1 Materialize fetched script source as `_daggerml_live.py`, create and register a file-backed `_daggerml_live` module, execute it through its source loader, and retrieve the configured function from the module.
- [x] 2.2 Match failed-import cleanup by removing `_daggerml_live` from `sys.modules` when module source execution raises, without changing the worker failure result contract.
- [x] 2.3 Configure an idempotent `_daggerml_live` DEBUG logger and stderr handler before module execution, disable propagation, and document that the supervisor's existing stderr pipe owns output capture.

## 3. Documentation

- [x] 3.1 Update built-in script integration documentation with live-module identity, source metadata, logger behavior, and the unchanged explicit dependency-injection boundary.
- [x] 3.2 Update Python authoring guidance with the `_daggerml_live` logger behavior and clarify that third-party dependency log levels are not enabled globally.

## 4. Verification

- [x] 4.1 Run the focused script executor and supervisor contract tests and fix regressions.
- [x] 4.2 Run repository lint/fix, type checking, and the non-slow test suite required by the contributor workflow.
- [x] 4.3 Run strict OpenSpec validation for `run-scripts-as-live-modules` and confirm all capability scenarios are covered by implementation or tests.
