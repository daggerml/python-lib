# CLI Design Contracts (`daggerml._cli`)

## Status

specified

## Authority

This document is authoritative for the internal subsystem contract described in this document.
If related docs conflict on this scope, this document is the source of truth.


## Purpose

CLI contracts define a minimal command wrapper around `DmlOps` with automatic configuration resolution and JSON output/error handling.

## Scope

CLI is an operational interface over `_internal` ops. It is not the canonical public Python API.
The CLI owns argument parsing, config resolution, and output normalization; operation semantics are delegated to `DmlOps` subsystems.

## Routing Model

- Top-level `dml` parser dispatches by operation group.
- Subcommands call the corresponding `DmlOps` subsystem methods via thin handler functions.
- The CLI does not reimplement commit/dag/index/cache/remote business logic.
- `dml contrib status` is a pass-through status surface over `daggerml.contrib.status.status()`.
- Errors are normalized into structured JSON error payloads.

## Behavior Contracts

- Default output is compact JSON for machine readability.
- Repo path resolution order: `--repo` -> `DML_REPO` -> cwd.
- Remote root resolution order: `--remote-root` -> `DML_REMOTE_ROOT`.
- Remote cache namespace resolution order: `--remote-cache` -> `DML_REMOTE_CACHE`.
- Verbosity controls logging level only.
- No command should emit unstructured tracebacks on expected domain errors.
- `remote` commands MUST use operation-oriented methods (`push|pull|list|prune|gc`) for remote sync/ref management.
- `remote` operations and `cache` operations are separate command domains.
- `contrib status` MUST emit the structured contrib status report as compact JSON.
- `cache` domain MUST support `list|get|put|delete|clear` operations.
- cache namespace constraints are defined in [remote-data-model.md](remote-data-model.md).
- cache command operation semantics (conflict/idempotence behavior) are defined in [remote-protocol.md](remote-protocol.md).
- runtime config naming follows [configuration.md](configuration.md): `remote.root` and `remote.cache`.

## Encoding/Decoding

`DmlJsonEncoder` defines wire-format conventions for refs and key internal datums (Uri/Runnable).

## Stability Notes

CLI surface is operational and can evolve faster than the `daggerml` Python package API.

## Content

See the sections in this document for normative content.

## References

None.
