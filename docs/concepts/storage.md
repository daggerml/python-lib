# Storage

DaggerML stores repository objects locally and refers to large external payloads indirectly.

## Local repository storage

The core repository store is LMDB-backed and organized around typed refs. Objects are written and read through namespaces such as `dag`, `commit`, `tree`, `node-*`, and `datum-*`.

A few consequences fall out of that design:

- identity is based on persisted refs, not Python object identity
- objects validate before write
- readers see complete object graphs across transaction boundaries
- shared sub-objects can be reused without copying

## Transactions and snapshots

Mutations happen inside explicit write transactions. Readers see valid snapshots rather than partial updates. That matches the rest of the model: indexes are mutable working state, while DAGs and commits are immutable records written from that state.

## Reachability and garbage collection

Because objects refer to each other through refs, DaggerML can reason about reachability. Branch heads and indexes act as the main roots for deciding which objects are still live.

Garbage collection removes repository objects that are no longer reachable from those roots.

## External data stays external

Not every value should live inside the repository. `Uri` values represent external locations such as files, object storage paths, or container/image targets.

The repository stores the reference and related bookkeeping, not the payload bytes themselves.

For cleanup-aware flows, DaggerML also has `Deletable` records that mark URI-backed resources as eligible for removal when the surrounding graph becomes unreachable.

## Local storage versus remote publication

Local repository storage is the working source of truth for a runtime. Remote publication is a separate concern layered on top of it. When objects are moved across process or machine boundaries, DaggerML can dump and reload object graphs or publish them through the remote CAS-plus-refs model.

See also:

- [Refs and namespaces](refs-and-namespaces.md)
- [Remotes](remotes.md)
- [Codecs and values](codecs-and-values.md)
