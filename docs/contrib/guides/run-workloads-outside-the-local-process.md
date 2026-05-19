# Run Workloads Outside The Local Process

Contrib wrappers compose, so the same logical function can be pushed through different backends.

## Docker

The repository example `examples/01-docker_dataset.py` shows the full pattern:

1. Build a container image with `daggerml.contrib.funks.docker_build`.
2. Wrap a function with `@api.funkify(uri="docker", image=...)`.
3. Keep the innermost function script-backed so the container runs a nested adapter.

Typical shape:

```python
@api.funkify(uri="docker", image=api.ref("image"), flags=api.ref("dkr-flags"))
@api.funkify
def download_dataset(dag):
    from sklearn.datasets import load_iris
    return load_iris(as_frame=True).frame.dropna()
```

Use Docker when the worker needs dependencies that you do not want in the local Python environment.

## SSH

`examples/02-ssh_docker_dataset.py` adds an SSH wrapper around the Docker-backed function:

```python
@api.funkify(uri="ssh", adapter="local", host=..., flags=..., env_files=...)
@api.funkify(uri="docker", image=api.ref("image"), flags=api.ref("dkr-flags"))
@api.funkify
def predict_target(dag, dataset, params):
    ...
```

The SSH executor opens one SSH session, sources each `env_file`, and runs the nested adapter with `--poll`. It does not create a separate remote wrapper script or a contrib-managed remote workdir.

## Batch

Use the `batch` executor when the nested work should run in AWS Batch but the orchestration boundary stays Lambda-based.

You supply:

- `lambda_uri`
- `image`
- optional `cpu`
- optional `memory`
- optional `gpu`

At runtime, the Lambda-side executor writes the nested adapter payload to S3, submits a Batch job, and later polls Batch for completion.

## CloudFormation

Use `cfn` when the result you want is a stack operation rather than a generic worker process. The executor creates or updates the stack, polls for terminal status, and commits stack outputs back as a DAG result.

## Choosing a backend

- Use `script` for the simplest local or subprocess-backed path.
- Use `docker` when the environment is the main problem.
- Use `ssh` when the machine boundary matters.
- Use `batch` when you need queued container execution in AWS.
- Use `cfn` when the workflow itself is infrastructure provisioning.

For exact kwargs and runtime behavior, see [reference/runtime-surfaces.md](../reference/runtime-surfaces.md).
