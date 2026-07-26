# Run work remotely

Remote-backed execution requires an S3 `remote.root` and credentials available to the execution boundary.

```bash
dml config set remote.root s3://bucket/research
dml config show
```

Supported composition includes script, Docker, SSH-wrapped Docker, AWS Batch through a Lambda adapter, and CloudFormation workloads. Select them by wrapping a funk; for example, `@api.funkify(uri="ssh", adapter="local", host=..., flags=...)` can wrap a Docker-backed funk.

SSH runs the nested adapter over the target host and can source `env_files`. AWS Batch additionally needs the Lambda integration, a container image, and its AWS prerequisites. These are researcher composition workflows; implementing an executor or adapter is extension work.
