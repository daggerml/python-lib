# Contrib Docs Overview

## Status

specified

## Authority

This document is authoritative for contrib-docs orientation only.
It defines where to start and which contrib docs to read by task.

Contrib architecture and module ownership contracts remain authoritative in [README.md](README.md).
Contrib runtime contracts remain authoritative in [runtime-contract.md](runtime-contract.md) and [execution-graph.md](execution-graph.md).

## Purpose

Provide a fast entrypoint to the contrib docs set so readers can pick the right contract doc first.

## Start Here

- First read [README.md](README.md) for contrib module boundaries and planning status.
- Then read [runtime-contract.md](runtime-contract.md) for canonical adapter/executor runtime behavior.

## Reading Paths By Task

- Building or changing adapter/executor runtime behavior:
  - [runtime-contract.md](runtime-contract.md)
  - [execution-graph.md](execution-graph.md)
  - [executor-catalog.md](executor-catalog.md)
  - [executor-state.md](executor-state.md)
  - [registries.md](registries.md) (only when changing registry/discovery contracts)
- Working on contrib status or diagnostics surfaces:
  - [status.md](status.md)
  - [registries.md](registries.md)
  - [../codec-system.md](../codec-system.md)
- Working on contrib codecs:
  - [codecs.md](codecs.md)
  - [../codec-system.md](../codec-system.md)
  - [s3-store.md](s3-store.md)
  - [../storing-and-retrieving-external-data.md](../storing-and-retrieving-external-data.md)
- Working on contrib funks:
  - [funks.md](funks.md)
  - [api.md](api.md)
  - [s3-store.md](s3-store.md)
  - [../storing-and-retrieving-external-data.md](../storing-and-retrieving-external-data.md)
- Working on `@funkify` or runnable construction:
  - [api.md](api.md)
  - [runtime-contract.md](runtime-contract.md)
- Working on contrib testing helpers:
  - [testing.md](testing.md)
  - [api.md](api.md)
- Working on class-based contrib DAG authoring:
  - [api.md](api.md)
  - [runtime-contract.md](runtime-contract.md)
- Working on S3 contrib utility behavior:
  - [s3-store.md](s3-store.md)
  - [../storing-and-retrieving-external-data.md](../storing-and-retrieving-external-data.md)
  - [../remote-data-model.md](../remote-data-model.md)

## Quick Ownership Map

- Module boundaries and dependency direction: [README.md](README.md)
- Canonical contrib runtime contracts: [runtime-contract.md](runtime-contract.md)
- Contrib live execution graph and cancel/sweep behavior: [execution-graph.md](execution-graph.md)
- Per-executor runtime contracts: [executor-catalog.md](executor-catalog.md)
- Focused registry contracts: [registries.md](registries.md)
- Contrib codec catalog and serialization behavior: [codecs.md](codecs.md)
- Contrib prebuilt funk surfaces: [funks.md](funks.md)
- Contrib testing helpers for author-code unit tests: [testing.md](testing.md)
- Focused executor-state contracts (shared record schema/ownership/metadata): [executor-state.md](executor-state.md)
- Contrib status/introspection contract: [status.md](status.md)
- `@funkify` and class-based DAG API: [api.md](api.md)
- `S3Store` behavior: [s3-store.md](s3-store.md)

## Scope

This document defines the scope described by its authority and purpose sections.

## Content

See the sections in this document for normative content.

## References

- [README.md](README.md)
- [runtime-contract.md](runtime-contract.md)
- [execution-graph.md](execution-graph.md)
- [status.md](status.md)
