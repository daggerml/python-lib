## Context

See `proposal.md` for motivation and `specs/script-module-execution/spec.md` for the behavioral contract. The script executor currently fetches generated source from S3, executes it with `exec(source, namespace)`, and retrieves the configured function from that dictionary. The worker already runs in a temporary DML project and the supervisor already captures the worker's stderr with a subprocess pipe.

The generated source and explicitly injected `extra_objs`/`post_lines` remain the complete script program. Authoring-process globals cannot be transferred into the worker, and installed dependencies remain the responsibility of the worker environment.

## Goals / Non-Goals

**Goals:**

- Give executed source the standard metadata and loader behavior of an imported top-level source module.
- Keep the live module separate from the installed `daggerml` package namespace.
- Make module DEBUG logging visible through existing supervisor stderr capture without enabling unrelated dependency loggers.
- Preserve script source, cache identity, function invocation, and terminal result behavior.

**Non-Goals:**

- Capture or serialize globals from the authoring process.
- Install third-party dependencies or include dependency versions in cache identity.
- Make the temporary module persist after the worker process exits.
- Change supervisor process, CloudWatch, cancellation, or result contracts.
- Enable DEBUG logging globally for DaggerML or third-party packages.

## Decisions

### Materialize and import a top-level `_daggerml_live` module

The worker will write the fetched source to `_daggerml_live.py` in its temporary project, build a file-backed import spec for module name `_daggerml_live`, create the module from that spec, register it in `sys.modules`, and execute it through the spec's source loader. The configured function will then be read from the module object.

Using a file-backed source loader gives the module ordinary `__name__`, `__file__`, `__package__`, `__loader__`, and `__spec__` values, gives frames a real source filename, and lets imports of `_daggerml_live` resolve during module initialization. Registration occurs before source execution, matching Python import behavior. If source execution fails, the worker removes the partially initialized module from `sys.modules` before propagating the error, also matching failed-import behavior.

Alternative considered: compile the source and execute it into a `ModuleType` dictionary. This supplies a name and can support `sys.modules`, but requires manually approximating loader and file metadata and remains observably different from a normal source import.

Alternative considered: place the module under `daggerml.contrib`. Rejected because the live script is user code, not an installed DaggerML submodule, and occupying that namespace could collide with future package modules or imply unsupported package-relative behavior.

### Configure a dedicated module logger before source execution

The worker will obtain `logging.getLogger("_daggerml_live")`, set it and its new `StreamHandler` to DEBUG, direct the handler to `sys.stderr`, install a concise formatter, and disable propagation. It will place that logger in the module dictionary as `logger` before the loader executes the source. Exact formatter fields are an implementation choice rather than part of the capability contract.

This preserves the existing injected `logger` convenience. Calls to `logging.getLogger(__name__)` resolve the same configured logger. Disabling propagation prevents duplicate records if an ancestor/root handler is configured. Configuring only `_daggerml_live` avoids turning on verbose or potentially sensitive logs from dependencies.

The handler writes to stderr rather than adding an in-process capture pipe. The supervisor already launches the worker with `stderr=subprocess.PIPE`, drains it continuously, stores it locally, and streams it to CloudWatch. A separate in-process pipe would duplicate buffering and lifecycle responsibilities without improving capture. A code comment will record this boundary for future direct worker-side capture needs.

Alternative considered: configure the root logger at DEBUG through `DML_DEBUG`. Rejected because no such environment contract currently exists and root DEBUG would enable unrelated dependency logs. A configurable global logging policy can be proposed separately if needed.

### Keep module wrapping outside script serialization and cache identity

`ScriptExecutor._render_script` and the stored script artifact remain unchanged. Module materialization and logging happen only after the worker fetches the artifact, so existing source-based cache keys and runnable payloads do not change. No new `funkify` argument is introduced.

## Risks / Trade-offs

- [The live source file can create a `__pycache__` entry] -> Keep it inside the worker's temporary project, which is discarded with the worker environment.
- [User source can replace the injected `logger` name] -> Treat this like any module-level reassignment; `logging.getLogger(__name__)` still retrieves the configured logger.
- [A handler could be added twice if worker execution is reused unexpectedly] -> Add the dedicated handler idempotently or tag it so repeated setup does not duplicate output, even though the current worker executes one payload per process.
- [Module-global DEBUG output may be more verbose than current behavior] -> Scope DEBUG to `_daggerml_live`, leave dependency and root logger levels unchanged, and preserve existing supervisor streaming limits.
- [The fixed module name would collide if one process ran scripts concurrently] -> The current worker runs one payload per process; any future multi-script worker must introduce per-execution module isolation before changing that lifecycle.

## Migration Plan

1. Add contract tests for metadata, self-import, failure cleanup, traceback source names, and logger capture.
2. Replace anonymous namespace execution with file-backed module loading in the script worker.
3. Update script executor documentation to describe the live module and stderr logger behavior.
4. Roll back by restoring anonymous namespace execution; runnable artifacts and persisted data require no migration.
