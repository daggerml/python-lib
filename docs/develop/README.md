# Develop DaggerML

This path is for contributors changing DaggerML itself. It covers the repository
layout, local development, tests, and the stable architecture behind the public
Python and command-line surfaces.

## Start here

1. [Set up a development checkout](setup.md).
2. [Run and select tests](testing.md).
3. [Orient yourself in the codebase](codebase-map.md).
4. Read the [contributor guide](contributing.md) before opening a pull request.

## Investigations

- [Flaky CI: execution cancellation and Docker over SSH](flaky-ci-investigation-2026-08-23.md):
  observed failures, concurrency findings, and recommended stabilization work.

## Architecture

- [System overview](architecture/system-overview.md): layers and the principal data flows.
- [Public API and CLI](architecture/public-api-and-cli.md): public entrypoints and their boundary with the core.
- [DAG storage and types](architecture/dag-storage-and-types.md): persisted objects, refs, and local storage.
- [Execution and runtime state](architecture/execution-and-runtime-state.md): mutable DAG construction, caching, and execution coordination.
- [Remotes and sync](architecture/remotes-and-sync.md): S3 transport, project sync, and remote cache state.
- [Local research dashboard](architecture/dashboard.md): local server, browser
  workbench, read model, versioned API, and cancellation boundary.

This is contributor-facing product documentation. Repository workflow policy,
agent instructions, edit maps, and change-planning governance remain in their
canonical maintainer files outside `docs/`.
