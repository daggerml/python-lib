# Contrib

`daggerml.contrib` is the extension layer around the core DaggerML API. It adds delayed authoring helpers, adapters and executors, plugin registries, testing helpers, dataframe codecs, and S3-backed artifact utilities.

This section is for readers who need to build or run contrib-backed DAGs, or who need to understand how contrib extends the runtime.

Back to the [main docs home](../README.md).

## Start here

- [Concepts](concepts/README.md): how delayed contrib authoring, runnable lowering, and execution backends fit together.
- [Guides](guides/README.md): practical paths for writing, testing, and running contrib workloads.
- [Reference](reference/README.md): exact Python API, runtime surfaces, registries, status output, codecs, and `S3Store` behavior.
- [Architecture](architecture/README.md): how the contrib runtime is wired internally.

## Fast paths

- New to contrib: read [concepts/authoring-and-runnables.md](concepts/authoring-and-runnables.md), then [guides/write-and-test-a-funk.md](guides/write-and-test-a-funk.md).
- Running work outside the local Python process: read [concepts/runtime.md](concepts/runtime.md), then [guides/run-workloads-outside-the-local-process.md](guides/run-workloads-outside-the-local-process.md).
- Checking exact adapter, executor, or plugin details: go to [reference/runtime-surfaces.md](reference/runtime-surfaces.md).
- Working on contrib internals: start from [architecture/execution-flow.md](architecture/execution-flow.md).

## What lives in contrib

- Authoring helpers in `daggerml.contrib.api`: `funkify`, `dagclass`, `run`, `ref`, and `load`.
- Adapters in `daggerml.contrib.adapters`: the built-in `local` and `lambda` adapter entrypoints.
- Executors in `daggerml.contrib.executors`: `script`, `docker`, `ssh`, `batch`, and `cfn`.
- Utilities in `daggerml.contrib.s3`, `daggerml.contrib.codecs`, `daggerml.contrib.funks`, `daggerml.contrib.testing`, and `daggerml.contrib.status`.
