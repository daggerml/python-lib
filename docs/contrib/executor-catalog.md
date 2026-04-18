---
status: specified
doc_type: spec
---

# Executor Catalog

## Authority

This document is authoritative for per-executor runtime behavior in contrib.

## Scope

This document defines per-executor runtime behavior for the contrib executor surfaces in this repository.

For each executor entry, this document defines:

- accepted kwargs,
- `resolve_runnable` behavior,
- runtime invocation behavior,
- implementation status when the executor is not yet implemented.

## Purpose

Define concise runtime contracts for each contrib executor.

## Glossary

- **S3Store**: An internal artifact store contract used to persist execution scripts and docker images.
- **Uri**: A universal resource identifier representing a runnable asset or storage location.
- **Runnable**: The primitive execution node resolved by an executor.
- **Supervisor**: The process and workspace runtime harness used to orchestrate script and worker lifecycles.

## Contract

### Interfaces

#### `script` executor

**Accepted kwargs**

- `fn`, `prepop`, `extra_objs`, `extra_lines`.

**Invocation Surfaces**

- `script` executor passes supervisor `cmd` as `["python", "-m", "daggerml.contrib.executors.script", "<argv-ptr>"]` for script execution.
- Script worker entrypoint accepts only `argv_ptr`.

**Behavior/Semantics**

- Script executor kickoff uses contrib supervisor as process/workspace runtime harness.
- Script executor starts supervisor in a detached process/session group and stores supervisor pid/paths in executor state.
- Supervisor starts the script worker in the same process group, so process-group termination from script executor cleanup propagates to supervisor and worker subtree.
- Supervisor, not the script worker, owns worker environment preparation.
- Script worker MUST create an isolated temporary working directory and change the worker current working directory to that location before user function execution.
- Script worker resolves the script runnable from `argv_ptr` by following the runnable linked list (`runnable.sub`) to its terminal node.
- Script worker reads script metadata from that terminal runnable (`script_uri`, `fn_name`, `prepop`).
- Script execution resolves script source from script runnable metadata via S3Store.
- Script execution runs against `argv_ptr` and enforces first-arg `dag` semantics.
- Script execution resolves `prepop` from script runnable metadata at the inner-most runnable in `argv[0]` and inserts named nodes before user function invocation.
- Script execution invokes user function as `fn(dag, *argv[1:], **call_kwargs)` where `call_kwargs` are derived from script runnable metadata.
- Script worker MUST instantiate DAG execution in a context-manager scope so DAG error handling is applied.
- Script worker computes `val = fn(...)` and, when the DAG is still uncommitted, commits `val` before calling `dag.cache()`.
- Script executor lifecycle is stateful kickoff/poll (`start/poll`) keyed by `cache_key`.
- Script executor `start` initializes canonical state records via executor-state APIs before background runtime handoff.
- Supervisor `run` loop performs heartbeat state updates through executor-state APIs while script execution is running.
- Script executor `poll` is read-only over state and only performs stale process-group termination safety checks.
- Script executor `cleanup` removes supervisor-owned residue after terminal execution.

#### `docker` executor

**Accepted kwargs**

- `image`, `sub`, optional `flags`.
- `image` MUST resolve to one of:
  - a `Uri` naming an S3-stored docker image tar produced by `docker_build`, or
  - a pushed image `Uri` produced by `docker_build`.
- `flags` MUST be `list[str]` when provided.

**Invocation Surfaces**

- Container invocation MUST call the nested adapter executable directly with mounted input/output file arguments and adapter CLI polling enabled.
- Container invocation MUST pass a child execution `cache_key` distinct from the parent executor `cache_key` to the nested adapter payload.
- Container invocation MUST provide the environment needed for nested `ExecutionState` access under the DynamoDB-backed design, including `DML_DYNAMODB_TABLE`, `DML_REMOTE_ROOT`, and required AWS environment.

**Behavior/Semantics**

- Implemented in `daggerml.contrib.executors.docker` for adapter `local`.
- Runtime behavior is stateful contrib executor kickoff/poll against a locally managed Docker container.
- `start` MUST require nested sub-runnable adapter `dml-local-adapter`.
- `start` MUST derive a child execution `cache_key` distinct from the parent executor `cache_key` and use that child key for the nested adapter payload executed in the container.
- `start` MUST write nested adapter input/output paths into a temporary work directory, start `docker run` with that directory mounted, and record container id plus temp-path state.
- When `image` is an S3 tar `Uri`, `start` MUST load that tar into the local Docker daemon before container launch.
- Docker executor state MUST store enough metadata to reopen that nested child State record in later poll invocations.
- `poll` MUST reopen that nested adapter-reported State record from stored metadata and read nested state to determine whether the nested execution is running or terminal.
- `poll` MUST project terminal child `ExecutionState` `dag_id` or `error` onto the parent state once the child reaches a terminal state.
- `poll` MUST report `running` while nested state is non-terminal and heartbeat is fresh.
- `poll` operates from executor-owned state metadata only; it does not require `argv_ptr`, `remote`, or `runnable` inputs.
- `cleanup` MUST be idempotent.
- `cleanup` MUST remove the container and temporary directory.
- `cleanup` MUST also remove any temporary image loaded from an S3 tar artifact.

#### `batch` executor

**Accepted kwargs**

- `lambda_uri`, `image`, optional `cpu`, optional `memory`, optional `gpu`; nested `sub` runnable is required separately.
- `image` MUST be a `Uri` naming the Batch container image.

**Behavior/Semantics**

- Implemented in `daggerml.contrib.executors.batch` for adapter `lambda`.
- Runtime behavior is stateful kickoff/poll against AWS Batch, with the executor handler itself running inside Lambda.
- Because the executor runs inside Lambda, Batch state handoff MUST use `ExecutionState` rather than process-local state.
- `resolve_runnable` MUST lower to `Runnable(target=<lambda_uri>, adapter="dml-lambda-adapter", ...)` and preserve the nested `sub` runnable chain.
- `start` MUST require nested sub-runnable adapter `dml-local-adapter`.
- `start` MUST derive a child execution `cache_key` distinct from the parent executor `cache_key` and use that child key for the nested adapter payload submitted to Batch.
- `start` MUST upload the nested adapter payload to S3 under the configured remote root, register a Batch job definition for the configured container image, and submit the job to `CPU_QUEUE` or `GPU_QUEUE` based on requested GPU count.
- `start` MUST persist the child execution identity and Batch job identifiers in executor metadata.
- Batch container execution MUST invoke the nested adapter directly as `<sub-adapter> --poll -i <s3-input-uri> -o /dev/null`.
- `poll` MUST inspect Batch job status and project terminal child `ExecutionState` `dag_id` or `error` onto the parent state once the Batch job reaches a terminal status.
- `cleanup` MUST be idempotent and SHOULD terminate or cancel the recorded Batch job and deregister the temporary Batch job definition.

#### `cfn` executor

**Accepted kwargs**

- Template/application payload sufficient to derive stack `name`, template body, and stack parameters.

**Behavior/Semantics**

- Implemented in `daggerml.contrib.executors.cfn` for adapter `local`.
- Runtime behavior is stateful contrib executor kickoff/poll against AWS CloudFormation.
- `start` MUST inspect current stack state, choose create or update, submit the stack operation, and record stack identifiers in executor state.
- `poll` MUST inspect stack status until it reaches a terminal success or failure state.
- On terminal success, runtime MUST materialize stack outputs into DAG-visible values and return those outputs as the execution result.

#### `ssh` executor

**Accepted kwargs**

- `host`, optional `flags`, optional `env_files`; nested `sub` runnable is required separately.

**Behavior/Semantics**

- Runtime behavior is stateful SSH transport around a nested child execution identity.
- `start` MUST require nested sub-runnable adapter `dml-local-adapter`.
- `start` MUST derive a child execution `cache_key` distinct from the parent executor `cache_key` and use that child key for nested remote adapter invocations.
- `start` MUST persist enough parent metadata to let later `poll` invocations reissue the same SSH transport step with the same child execution identity.
- `start` MUST open one SSH session to `host`, forward the nested adapter payload on stdin, and return the nested adapter's canonical `{status, error}` output.
- `start` MUST source each `env_file` in order before invoking the nested adapter command.
- `start` MUST invoke the nested adapter as a direct command over SSH; it MUST NOT set contrib-specific environment variables, write remote wrapper scripts, or create remote working directories.
- `poll` MUST reissue the SSH transport step using the persisted child execution identity until the child reaches a terminal state.
- On nested terminal success or failure, the parent execution MUST project the child `ExecutionState` terminal `dag_id` or `error` onto the parent state.
- `cleanup` is a no-op lifecycle hook because the executor retains no external runtime handle beyond executor-state metadata.

### Invariants

#### `script` executor

- Script executor runnable resolution serializes `fn` together with `extra_objs` and `extra_lines` into an executable script.
- `script` executor requires `sub == None`; script executor does not dispatch to `sub` runnable chains.
- Script source rendering strips function decorators when materializing executable source.
- `resolve_runnable` MUST validate that the generated script parses as valid Python.
- `resolve_runnable` MUST validate that `fn` is defined at module global scope in the generated script.
- The script artifact is uploaded to S3.
- `resolve_runnable` parses `fn` signature metadata and computes `call_kwargs` for the returned runnable.
- The first `fn` parameter (`dag`) is runtime-provided by the script executor and MUST NOT be encoded into runnable args/kwargs.
- Parameters with default values MUST be treated as strict kwargs in derived `call_kwargs`, including positional-or-keyword parameters with defaults.
- Positional invocation values MUST bind only to required non-default parameters; positional values for defaulted parameters MUST be rejected.
- `resolve_runnable` returns runnable target as executor id (`script`) and script runtime kwargs with shape:
  - `{"__dml_script_exec__": {"prepop": ..., "fn_name": ..., "script_uri": ...}, **call_kwargs}`.
- `__dml_script_exec__` is reserved internal metadata namespace and MUST NOT be interpreted as user call kwargs.
- user call kwargs are flattened at top level of runnable kwargs and forwarded to runtime invocation.
- `resolve_runnable` MUST NOT perform additional delayed-action resolution; delayed-action resolution is owned by the codec system.
- Positional argv beyond `dag` is valid only for required non-default parameters.

#### `docker` executor

- `resolve_runnable(uri, kwargs, sub)` MUST require `sub != None` plus container image metadata.
- `resolve_runnable` MUST reject unknown kwargs.
- `resolve_runnable` MUST preserve the nested `sub` runnable chain and runtime container flags.
- `resolve_runnable` MUST return runnable target `docker` with adapter `dml-local-adapter`.

#### `cfn` executor

- `resolve_runnable(uri, kwargs, sub)` SHOULD package CloudFormation template/application data directly and SHOULD require `sub == None`.
- `resolve_runnable` SHOULD preserve enough structured data to create or update a named stack and later materialize stack outputs.

#### `batch` executor

- `resolve_runnable(uri, kwargs, sub)` MUST require `sub != None`, `lambda_uri`, and Batch container resource metadata.
- `resolve_runnable` MUST preserve the nested `sub` runnable chain.
- `resolve_runnable` MUST return runnable target `<lambda_uri>` with adapter `dml-lambda-adapter`.

#### `ssh` executor

- `resolve_runnable(uri, kwargs, sub)` MUST require `sub != None` plus remote-host connection metadata.
- `resolve_runnable` MUST preserve the nested `sub` runnable chain.
- `resolve_runnable` MUST NOT synthesize wrapper-script payloads or executor-owned environment metadata.

### Error Semantics

#### `script` executor

- Deterministic failure on function/object serialization errors.
- Deterministic failure when generated script is not valid Python.
- Deterministic failure when `fn` is not globally defined in the generated script.
- Deterministic failure on script upload-to-S3 errors.
- Deterministic failure on invalid script kwargs shape.
- Deterministic failure on any unrecognized script-executor kwargs.
- Deterministic failure when positional invocation values map to defaulted parameters.
- Deterministic failure when inner-most `sub`/`prepop` extraction is invalid.

#### `docker` executor

- Deterministic failure on invalid image/sub kwargs shape.
- Deterministic failure when nested sub-runnable adapter is not `dml-local-adapter`.
- Deterministic failure when Docker is unavailable or container startup fails.
- `poll` MUST fail deterministically when nested heartbeat becomes stale.

#### `cfn` executor

- Deterministic failure on invalid stack/template/parameter payload shape.
- Deterministic failure on CloudFormation create/update/describe errors.
- Deterministic failure when stack creation or update reaches a rollback/failure terminal state.

#### `batch` executor

- Deterministic failure on invalid `lambda_uri`/image/sub/resource kwargs shape.
- Deterministic failure when required Batch environment/configuration is missing (`CPU_QUEUE`, `GPU_QUEUE`, `BATCH_TASK_ROLE_ARN`, or an S3-backed remote root).
- Deterministic failure on Batch job-definition registration, submission, describe, cancel, or terminate errors.
- Deterministic failure when a terminal Batch job produces no valid canonical adapter result payload.

#### `ssh` executor

- Deterministic failure on invalid host/sub kwargs shape.
- Deterministic failure on SSH command or connection failures.
- Deterministic failure when sourced `env_files` or remote nested execution prevent canonical adapter JSON output from being returned.

### Security Boundaries

None identified in this spec. Handled by generic execution environment assumptions.

### Observability

- Stateful executor status is observable via executor state logs and heartbeat files as outlined in the runtime behaviors.
- `batch` executor observability includes recorded Batch job id, job-definition arn, and S3 result/error object locations in executor metadata.
- `ssh` executor observability includes the persisted child execution identity and SSH transport metadata needed to resume polling.

### Authority Handoffs

None.

## Compatibility

- Runtime behavior implementations for `script`, `docker`, `batch`, and `cloudformation` must maintain backward compatibility for kwargs and executor state structures.
- `ssh` runtime behavior in this document defines the current contrib contract.

## References

- [api.md](api.md)
- [runtime-contract.md](runtime-contract.md)
