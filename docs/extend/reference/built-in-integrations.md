# Built-In Integrations

| Integration | Adapter | Purpose |
| --- | --- | --- |
| `script` | `local` | serialize and run a Python function through the supervisor |
| `docker` | `local` | run a nested adapter in a detached Docker container |
| `ssh` | `local` | run a nested adapter synchronously over SSH |
| `batch` | `lambda` | submit a nested adapter to AWS Batch through Lambda |
| `cfn` | `local` | create or update a CloudFormation stack and return outputs |

## Script source injection

`ScriptExecutor` accepts only `fn`, `prepop`, `extra_objs`, and `post_lines`.
`extra_lines` is not a supported argument. It serializes the inspectable source
of every `extra_objs` object first, then the function source, then literal
`post_lines`, and validates the combined text as Python.

The script worker has only that rendered source and its own injected namespace;
module globals and imports from the authoring process are not transferred.
Put imports inside the function, include source-defined helpers in `extra_objs`,
or use `post_lines` deliberately to create names needed at call time. The first
function parameter must be named `dag`, and the rendered function must be a
global definition. `script` cannot wrap a `sub` runnable.

## Docker, SSH, and Batch

Docker, SSH, and Batch require a nested `sub` runnable. Docker requires an
image and optionally flags; it can load an image tar from an S3 URI. SSH requires
a non-empty host and accepts flags and environment files. Batch requires a
Lambda URI and image, with optional CPU, memory, and GPU values; its deployment
also needs `CPU_QUEUE` or `GPU_QUEUE` and `BATCH_TASK_ROLE_ARN` environment
configuration.

## CloudFormation

`cfn` is a narrow executor, not a general CloudFormation helper. It expects the
three callable arguments `(name, template, params)`, JSON-encodes the template,
and creates or updates the named stack with only `CAPABILITY_IAM` and
`CAPABILITY_NAMED_IAM`. It returns stack outputs in a committed DAG.

It does not expose general CloudFormation request options such as tags, roles,
notification ARNs, change sets, template URLs, or configurable capabilities.
On cancellation it first attempts `cancel_update_stack` and then attempts stack
delete if that fails. Treat it as best-effort and verify the resulting stack in
CloudFormation.

## Dataframe codecs and S3Store

Contrib supplies optional pandas and polars dataframe codecs that write Parquet
through `S3Store`. `S3Store` also supports content-addressed bytes/files, JSON,
listing, removal, and deterministic tar/untar operations for integration
artifacts.
