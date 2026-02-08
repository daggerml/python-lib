---
status: specified
doc_type: spec
---

# Executor Catalog

## Authority

This document is authoritative for per-executor runtime behavior in contrib.

## Purpose

Define concise runtime contracts for each contrib executor.

## Scope

This document defines per-executor runtime behavior for the currently implemented contrib executor and the currently planned unimplemented executor surfaces in this repository.

For each executor entry, this document defines:

- accepted kwargs,
- `resolve_runnable` behavior,
- runtime invocation behavior,
- implementation status when the executor is not yet implemented.

## Content

### Contracts

#### `script` executor

**Accepted kwargs**

- `fn`, `prepop`, `extra_objs`, `extra_lines`.

**Runnable resolution**

- Script executor runnable resolution serializes `fn` together with `extra_objs` and `extra_lines` into an executable script.
- `script` executor requires `sub == None`; script executor does not dispatch to `sub` runnable chains.
- Script source rendering strips function decorators when materializing executable source.
- `resolve_runnable` MUST validate that the generated script parses as valid Python.
- `resolve_runnable` MUST validate that `fn` is defined at module global scope in the generated script.
- The script artifact is uploaded to S3 (the current artifact store contract).
- `resolve_runnable` parses `fn` signature metadata and computes `call_kwargs` for the returned runnable.
- The first `fn` parameter (`dag`) is runtime-provided by the script executor and MUST NOT be encoded into runnable args/kwargs.
- Parameters with default values MUST be treated as strict kwargs in derived `call_kwargs`, including positional-or-keyword parameters with defaults.
- Positional invocation values MUST bind only to required non-default parameters; positional values for defaulted parameters MUST be rejected.
- `resolve_runnable` returns runnable target as executor id (`script`) and script runtime kwargs with shape:
  - `{"__dml_script_exec__": {"prepop": ..., "fn_name": ..., "script_uri": ...}, **call_kwargs}`.
- `__dml_script_exec__` is reserved internal metadata namespace and MUST NOT be interpreted as user call kwargs.
- user call kwargs are flattened at top level of runnable kwargs and forwarded to runtime invocation.
- `resolve_runnable` MUST NOT perform additional delayed-action resolution (for example `api.ref`/`api.load`); delayed-action resolution is owned by the codec system.

**Runtime behavior**

- Script executor kickoff uses contrib supervisor as process/workspace runtime harness.
- Script executor passes supervisor `cmd` as `["python", "-m", "daggerml.contrib.executors.script", "<argv-ptr>"]` for script execution and reuses the same `cache_key` lifecycle across polls.
- Script executor starts supervisor in a detached process/session group and stores supervisor pid/paths in executor state.
- Supervisor starts the script worker in the same process group, so process-group termination from script executor kill propagates to supervisor + worker subtree.
- Supervisor, not the script worker, owns worker environment preparation.
- Script worker MUST create an isolated temporary working directory and change the worker current working directory to that location before user function execution.
- The script worker entrypoint accepts only `argv_ptr`; all other runtime setup is assumed to have been prepared by the supervisor.
- Script worker resolves the script runnable from `argv_ptr` by following the runnable linked list (`runnable.sub`) to its terminal node.
- Script worker reads script metadata from that terminal runnable (`script_uri`, `fn_name`, `prepop`).
- Script execution resolves script source from script runnable metadata via S3Store.
- Script execution runs against `argv_ptr` and enforces first-arg `dag` semantics.
- Script execution resolves `prepop` from script runnable metadata at the inner-most runnable in `argv[0]` and inserts named nodes before user function invocation.
- Script execution invokes user function as `fn(dag, *argv[1:], **call_kwargs)` where `call_kwargs` are derived from script runnable metadata.
- Positional argv beyond `dag` is valid only for required non-default parameters.
- Script worker MUST instantiate DAG execution in a context-manager scope so DAG error handling is applied.
- Script worker computes `val = fn(...)` and, when the DAG is still uncommitted, commits `val` before calling `dag.cache()`.
- Script executor lifecycle is stateful kickoff/poll (`start/poll`) keyed by `cache_key`.
- Script executor declares `state_class = LocalState` for supervisor/state backend selection.
- Script executor MUST either propagate received state/comms configuration to sub-execution steps or update that state directly.
- Script executor remains the logical run owner (`owner_executor = "script"`) when it is the deepest runnable executor.
- Script executor `start` initializes canonical state records via executor-state APIs before background runtime handoff.
- Supervisor `run` loop performs heartbeat/lease state updates through executor-state APIs while script execution is running.
- Script executor `poll` is read-only over state and only performs stale process-group kill safety checks.

**Failure contracts**

- Deterministic failure on function/object serialization errors.
- Deterministic failure when generated script is not valid Python.
- Deterministic failure when `fn` is not globally defined in the generated script.
- Deterministic failure on script upload-to-S3 errors.
- Deterministic failure on invalid script kwargs shape.
- Deterministic failure on any unrecognized script-executor kwargs.
- Deterministic failure when positional invocation values map to defaulted parameters.
- Deterministic failure when inner-most `sub`/`prepop` extraction is invalid.

#### `docker` executor

**Implementation status**

- Implemented in `daggerml.contrib.executors.docker` for adapter `local`.

**Accepted kwargs**

- `image`, `sub`, optional `flags`.
- `image` MUST resolve to one of:
  - a `Uri` naming an S3-stored docker image tar produced by `docker_build`, or
  - a pushed image `Uri` produced by `docker_build`.
- `flags` MUST be `list[str]` when provided.

**Runnable resolution**

- `resolve_runnable(uri, kwargs, sub)` MUST require `sub != None` plus container image metadata.
- `resolve_runnable` MUST reject unknown kwargs.
- `resolve_runnable` MUST preserve the nested `sub` runnable chain and runtime container flags.
- `resolve_runnable` MUST return runnable target `docker` with adapter `dml-local-adapter`.

**Runtime behavior**

- Runtime behavior is stateful contrib executor kickoff/poll against a locally managed Docker container.
- `start` MUST require nested sub-runnable adapter `dml-local-adapter`.
- `start` MUST write nested adapter input/output paths plus a mounted local state directory into a temporary work directory, start `docker run` with that directory mounted, and record container id plus temp-path state.
- When `image` is an S3 tar `Uri`, `start` MUST load that tar into the local Docker daemon before container launch.
- The container invocation MUST call the nested adapter executable directly with mounted input/output file arguments and adapter CLI polling enabled.
- The nested adapter payload MUST include `comms` naming the mounted local state directory as Parent Comms for the nested adapter invocation.
- That `comms` attachment applies only to the nested adapter invocation started by Docker; the child adapter MUST NOT propagate it further.
- Docker executor state MUST store enough metadata to reopen that nested adapter's reported State record in later poll invocations.
- `poll` MUST reopen that nested adapter-reported State record from stored metadata and read nested state to determine whether the nested execution is running or terminal.
- `poll` MUST report `running` while nested state is non-terminal and heartbeat is fresh.
- `poll` MUST return the nested adapter's terminal result after terminal state is reached and output is available.
- `poll` MUST fail deterministically when nested heartbeat becomes stale.
- `poll` MUST also fail deterministically when the container exits without a valid terminal output file.
- `poll` and `kill` operate from executor-owned state metadata only; they do not require `argv_ptr`, `remote`, or `runnable` inputs.
- `gc` MUST be idempotent.
- `kill`/`gc` MUST remove the container and temporary directory.
- `kill`/`gc` MUST also remove any temporary image loaded from an S3 tar artifact.

**Failure contracts**

- Deterministic failure on invalid image/sub kwargs shape.
- Deterministic failure when nested sub-runnable adapter is not `dml-local-adapter`.
- Deterministic failure when Docker is unavailable or container startup fails.
- Deterministic failure when the container exits without writing output.

#### `cloudformation` executor

**Implementation status**

- Not implemented yet in `daggerml.contrib`.
- This entry captures the intended contrib-facing shape derived from the legacy `dml-util` `CfnRunner` surface.

**Accepted kwargs**

- Template/application payload sufficient to derive stack `name`, template body, and stack parameters.

**Runnable resolution**

- `resolve_runnable(uri, kwargs, sub)` SHOULD package CloudFormation template/application data directly and SHOULD require `sub == None`.
- `resolve_runnable` SHOULD preserve enough structured data to create or update a named stack and later materialize stack outputs.

**Runtime behavior**

- Runtime behavior is stateful contrib executor kickoff/poll against AWS CloudFormation.
- `start` SHOULD inspect current stack state, choose create or update, submit the stack operation, and record stack identifiers in executor state.
- `poll` SHOULD inspect stack status until it reaches a terminal success or failure state.
- On terminal success, runtime materializes stack outputs into DAG-visible values and returns those outputs as the execution result.

**Failure contracts**

- Deterministic failure on invalid stack/template/parameter payload shape.
- Deterministic failure on CloudFormation create/update/describe errors.
- Deterministic failure when stack creation or update reaches a rollback/failure terminal state.

#### `ssh` executor

**Implementation status**

- Not implemented yet in `daggerml.contrib`.
- This entry captures the intended contrib-facing shape derived from the legacy `dml-util` `SshRunner` surface.

**Accepted kwargs**

- `host`, `sub`, optional `flags`, optional `env_files`.

**Runnable resolution**

- `resolve_runnable(uri, kwargs, sub)` SHOULD require `sub != None` plus remote-host connection metadata.
- `resolve_runnable` SHOULD synthesize remote wrapper script content that exports contrib environment/config values and optionally sources `env_files` before nested execution.
- `resolve_runnable` SHOULD preserve the nested `sub` runnable chain.

**Runtime behavior**

- Runtime behavior is direct remote execution over SSH rather than a long-running remote state backend by default.
- `start` SHOULD create a temporary remote script, invoke it as `<script> <sub-adapter> <sub-uri>`, and forward nested runnable kwargs on stdin.
- `start` SHOULD remove the temporary remote script after execution completes when execution is synchronous.
- If implemented as synchronous remote execution, `poll` MAY be trivial or terminal immediately after `start`; if implemented as background remote execution, `poll`/`kill` MUST resume the same remote job state keyed by `cache_key`.

**Failure contracts**

- Deterministic failure on invalid host/sub kwargs shape.
- Deterministic failure on SSH command, connection, or remote-script setup failures.
- Deterministic failure when remote nested execution exits non-zero.

## References

- [api.md](api.md)
- [runtime-contract.md](runtime-contract.md)
