# Built-In Integrations

| Integration | Adapter | Purpose |
| --- | --- | --- |
| `script` | `local` | serialize and run a Python function through the supervisor |
| `docker` | `local` | run a nested adapter in a detached Docker container |
| `ssh` | `local` | run a nested adapter synchronously over SSH |
| `batch` | `lambda` | submit a nested adapter to AWS Batch through Lambda |

## Script source injection

`ScriptExecutor` accepts only `fn`, `prepop`, `extra_objs`, and `post_lines`.
`extra_lines` is not a supported argument. It serializes the inspectable source
of every `extra_objs` object first, then the function source, then literal
`post_lines`, and validates the combined text as Python.

The worker materializes that source as `_daggerml_live.py` and imports it as the
top-level module `_daggerml_live`. The script therefore receives ordinary
`__name__`, `__file__`, `__package__`, `__loader__`, and `__spec__` metadata and
can resolve its in-progress module through `sys.modules`.

The module receives an injected `_daggerml_live` logger configured at DEBUG with
a stderr handler; `logging.getLogger(__name__)` resolves the same logger. This
configuration does not enable DEBUG output for dependency loggers. The
supervisor captures the worker's stderr through its existing process pipe.

The live module still has only the rendered source; module globals and imports
from the authoring process are not transferred. Put imports inside the function,
include source-defined helpers in `extra_objs`, or use `post_lines` deliberately
to create names needed at call time. The first function parameter must be named
`dag`, and the rendered function must be a global definition. `script` cannot
wrap a `sub` runnable.

## Docker, SSH, and Batch

Docker, SSH, and Batch require a nested `sub` runnable. Docker requires an
image and optionally flags; it can load an image tar from an S3 URI. SSH requires
a non-empty host and accepts flags and environment files. Batch requires a
Lambda URI and image, with optional CPU, memory, and GPU values; its deployment
also needs `CPU_QUEUE` or `GPU_QUEUE` and `BATCH_TASK_ROLE_ARN` environment
configuration.

## Dataframe codecs and S3Store

Contrib supplies optional pandas and polars dataframe codecs that write Parquet
through `S3Store`. `S3Store` also supports content-addressed bytes/files, JSON,
listing, removal, and deterministic tar/untar operations for integration
artifacts.
