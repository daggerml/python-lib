## Context

The current system already separates authoring-time adapter selection from executor dispatch, but it does not enforce that separation at the runtime boundary. Public APIs commonly accept symbolic adapter names such as `local`, codec normalization resolves those names through the adapter registry, and contrib executors typically emit concrete adapter strings such as `dml-local-adapter` or `dml-lambda-adapter`. However, `IndexOps` still contains a fallback path that treats an unresolved concrete adapter string as a hint to re-enter the adapter registry, import a Python object, and invoke its `cli()` entrypoint indirectly.

That fallback hides the real contract. The runtime should only see concrete command-line-callable adapter identities such as `dml-local-adapter`, `dml-lambda-adapter`, `python3`, `podman-adapter`, or `/opt/acme/bin/build-adapter`. Symbolic names such as `local`, `lambda`, or plugin-defined sugar such as `gpu` belong to authoring and normalization, not to runtime execution. The only intentional non-command adapter value is `adapter == ""` for explicit builtin-function execution paths such as `get` and `concat`, where the runtime checks for builtin behavior directly instead of shelling out.

## Goals / Non-Goals

**Goals:**
- Make the runtime adapter boundary explicit and fail closed when a concrete adapter command is unavailable.
- Preserve short symbolic adapter names as authoring-time sugar so users do not need to spell built-in adapter commands in normal API calls.
- Keep plugin extensibility intact for both adapter sugar and executor registration under existing built-in adapters.
- Document that concrete adapter identities need not start with `dml-`; built-in adapters happen to use that prefix, but plugin adapters may resolve to any command-line-callable string or explicit path.

**Non-Goals:**
- Introduce a new non-CLI adapter invocation mode.
- Require plugin adapters to use a `dml-` prefix.
- Change builtin execution to route through adapters when the runtime already handles builtin functions directly.
- Redesign executor dispatch semantics beyond clarifying the adapter-resolution boundary.

## Decisions

### 1. Runtime execution only accepts concrete adapter commands

The runtime boundary will treat `Runnable.adapter` as an operational command string, not as a symbolic registry key. By the time a runnable reaches `IndexOps` adapter execution, its adapter value must already be directly callable from the command line or be an explicit filesystem path.

Rationale:
- This makes runtime behavior deterministic across environments.
- It prevents a single concrete adapter string from taking different execution paths depending on whether a Python import fallback happens to succeed.
- It matches the intended transport boundary: adapters are CLI programs that own their own stdin/stdout contract.

Alternatives considered:
- Keep the Python import fallback: rejected because it silently repairs invalid runtime state and makes execution mechanism depend on environment accidents.
- Let `IndexOps` resolve symbolic adapter names at runtime: rejected because it leaks authoring sugar into the execution boundary.

### 2. Symbolic adapter names remain sugar resolved upstream

Author-facing APIs may continue to accept sugar such as `local`, `lambda`, or plugin-defined symbolic names. That sugar must resolve before runtime execution, through the adapter registry and normal runnable-resolution flow.

Examples:
- `local` -> `dml-local-adapter`
- `lambda` -> `dml-lambda-adapter`
- `gpu` -> `podman-adapter`
- `acme` -> `/opt/acme/bin/acme-adapter`

Rationale:
- This preserves ergonomics while keeping runtime semantics strict.
- It provides a clear extension point for plugin-defined sugar without changing the execution model.

Alternatives considered:
- Require users to always provide full adapter commands: rejected because it makes normal built-in usage unnecessarily verbose.

### 3. Builtin execution keeps its explicit empty-adapter exception

The only adapter value that may reach runtime without being command-line-callable is `""`, and only for explicit builtin-function execution paths where the runtime checks for builtin behavior directly and does not shell out.

Rationale:
- Builtins are not external adapters and already follow a separate runtime branch.
- Keeping this exception explicit prevents future confusion about whether empty adapter strings are generally allowed.

Alternatives considered:
- Represent builtins with a special command name: rejected because the runtime already has explicit builtin handling and does not need a fake adapter executable.

### 4. Missing concrete adapter commands are installation or configuration errors

If a resolved runnable names `dml-local-adapter`, `python3`, `podman-adapter`, or any other concrete command and that command is not callable from the runtime environment, execution must fail immediately rather than falling back to import-based recovery.

Rationale:
- This exposes packaging and environment problems where they actually exist.
- It keeps tests honest: built-in adapter scripts declared in `pyproject.toml` must be installed and available.

Alternatives considered:
- Allow fallback only for built-in adapters: rejected because it still weakens the invariant and creates a privileged special case for one packaging mode.

### 5. Adapter test fixtures must obey the same executability contract

Test fixtures that are referenced as adapters at runtime must themselves be executable command-line programs or explicit executable paths. For example, `tests/assets/internal_fn/python-fork-adapter.py` must carry an executable bit when tests pass its filesystem path as `Runnable.adapter`.

Rationale:
- This keeps tests aligned with the production runtime contract instead of relying on test-only behavior.
- It ensures path-based adapter coverage exercises the same `shutil.which(...)` or explicit-path execution path used in production.

## Risks / Trade-offs

- Missing adapter scripts in local dev or test environments will fail sooner -> Mitigation: treat this as desired signal and update tests/tooling to require installed console scripts.
- Some tests and helpers may still construct raw runnables with symbolic adapter names -> Mitigation: update those fixtures to reflect the runtime invariant and keep sugar only at authoring-time APIs.
- Plugin authors may assume importable adapter specs are enough -> Mitigation: document that plugin sugar must resolve to a concrete CLI adapter command or explicit executable path.
- The builtin empty-string exception could be overgeneralized later -> Mitigation: keep the exception narrow and document that it only applies to explicit builtin runtime branches.
