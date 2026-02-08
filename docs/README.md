# DaggerML Docs

## Status

specified

## Authority

This document is authoritative for the docs information architecture and navigation.
If related docs conflict on documentation layout or entry points, this document is the source of truth.


## Purpose

This index defines the docs layout and the canonical docs for each module and object area.


## Layout

- Core contracts and top-level module docs live in `docs/`.
- Contrib module docs live in `docs/contrib/`.
- Internal module docs live in `docs/internal/`.
- Internal ops subsystem docs live in `docs/internal/ops/`.


## Core Model Docs

- Object model: [object-model.md](object-model.md)
- DAG model: [dag-model.md](dag-model.md)
- Commit model: [commit-model.md](commit-model.md)
- Namespace model: [internal/namespace.md](internal/namespace.md)
- Storage model: [internal/storage.md](internal/storage.md)
- Storing and retrieving external data (`Uri`, `Deletable`): [storing-and-retrieving-external-data.md](storing-and-retrieving-external-data.md)
- Error model: [errors.md](errors.md)


## Execution Docs

- Configuration model: [configuration.md](configuration.md)
- Adapter execution contract: [adapter-execution-contract.md](adapter-execution-contract.md)
- Execution model: [execution-model.md](execution-model.md)
- Default runtime (`daggerml` module default `Dml`): [default-dml-runtime.md](default-dml-runtime.md)
- Codec system: [codec-system.md](codec-system.md)


## Remote Docs

- Remote sync lifecycle: [remote-sync.md](remote-sync.md)
- Remote data model (CAS+refs layout/schemas): [remote-data-model.md](remote-data-model.md)
- Remote protocol (sync operations): [remote-protocol.md](remote-protocol.md)


## Module Docs

- Documentation orientation overview: [spec/overview.md](spec/overview.md)
- System layering: [system.md](system.md)
- Public API module (`daggerml.api`): [api.md](api.md)
- CLI module (`daggerml._cli`): [cli.md](cli.md)
- Internal module (`daggerml._internal`): [internal/README.md](internal/README.md)
- Ops module (`daggerml._internal.ops`): [internal/ops/README.md](internal/ops/README.md)
- Contrib module (`daggerml.contrib`): [contrib/README.md](contrib/README.md)


## Internal Submodule Docs

- Internal namespace contracts: [internal/namespace.md](internal/namespace.md)
- Internal storage model: [internal/storage.md](internal/storage.md)
- Internal type-system contracts: [internal/type-system-contracts.md](internal/type-system-contracts.md)
- Internal storage and refs: [internal/storage-and-refs.md](internal/storage-and-refs.md)
- `DmlOps`: [internal/ops/dml-ops.md](internal/ops/dml-ops.md)
- `BaseOps`: [internal/ops/base-ops.md](internal/ops/base-ops.md)
- `HeadOps`: [internal/ops/head-ops.md](internal/ops/head-ops.md)
- `CommitOps`: [internal/ops/commit-ops.md](internal/ops/commit-ops.md)
- `DagOps`: [internal/ops/dag-ops.md](internal/ops/dag-ops.md)
- `IndexOps`: [internal/ops/index-ops.md](internal/ops/index-ops.md)
- `NodeOps`: [internal/ops/node-ops.md](internal/ops/node-ops.md)
- `CacheOps`: [internal/ops/cache-ops.md](internal/ops/cache-ops.md)
- `GcOps`: [internal/ops/gc-ops.md](internal/ops/gc-ops.md)
- `RemoteOps`: [internal/ops/remote-ops.md](internal/ops/remote-ops.md)


## Contrib Submodule Docs

- contrib docs overview / where to start: [contrib/overview.md](contrib/overview.md)
- contrib runtime contract: [contrib/runtime-contract.md](contrib/runtime-contract.md)
- contrib executor catalog: [contrib/executor-catalog.md](contrib/executor-catalog.md)
- contrib codecs: [contrib/codecs.md](contrib/codecs.md)
- contrib funks: [contrib/funks.md](contrib/funks.md)
- contrib testing helpers: [contrib/testing.md](contrib/testing.md)
- `daggerml.contrib.api` surface (`@dagclass`, `@funkify`, delayed actions, run): [contrib/api.md](contrib/api.md)
- contrib registries (adapter/executor): [contrib/registries.md](contrib/registries.md)
- contrib executor state: [contrib/executor-state.md](contrib/executor-state.md)
- contrib status/introspection API: [contrib/status.md](contrib/status.md)
- `daggerml.contrib.s3`: [contrib/s3-store.md](contrib/s3-store.md)

## Scope

This document defines the scope described by its authority and purpose sections.

## Content

See the sections in this document for normative content.

## References

None.
