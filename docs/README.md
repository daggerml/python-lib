# DaggerML docs

This `docs/` tree is the human-facing documentation for DaggerML.

If you want change proposals, task lists, or planning artifacts for work on the repo, look in `openspec/` instead. `openspec/` is for change planning; `docs/` is for learning and using the project.

## Start here

- [Getting started](getting-started.md): install DaggerML, create a repo, make a first DAG, and inspect it.
- [Concepts](concepts/README.md): the core ideas behind DAGs, commits, refs, execution, storage, remotes, and values.
- [Guides](guides/README.md): task-focused walkthroughs built around real DaggerML workflows.
- [Reference](reference/README.md): the exact Python API, CLI, configuration, and error surfaces.
- [Architecture](architecture/README.md): how the system is put together internally for advanced readers and contributors.
- [Contrib](contrib/README.md): contrib-specific APIs, runtime pieces, and supporting docs.

## What DaggerML exposes

- A Python API centered on `Dml`, `Dag`, `Node`, `Ref`, and helpers such as `new()` and `load()`.
- A CLI centered on `dml` commands for repo, DAG, commit, config, and related inspection workflows.
- A contrib package for adapters, executors, codecs, and helper APIs that extend the core runtime.

## Reading path

Start with [Getting started](getting-started.md) if you want a first working repo. After that, move by question:

- use [Concepts](concepts/README.md) for mental models
- use [Guides](guides/README.md) for workflows
- use [Reference](reference/README.md) for exact commands and APIs
- use [Architecture](architecture/README.md) when you need the implementation picture
