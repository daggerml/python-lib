# CLI Design Contracts (`daggerml._cli`)

## Status

specified

## Authority

This document is authoritative for CLI subsystem contracts.

## Purpose

Define the minimal command wrapper around `DmlOps` with automatic configuration resolution and JSON output and error handling.

## Scope

CLI is an operational interface over `_internal` ops. It owns argument parsing, config resolution, and output normalization; operation semantics are delegated to `DmlOps` subsystems.

## Routing Model

- top-level `dml` dispatches by operation group,
- subcommands call corresponding `DmlOps` methods via thin handlers,
- the CLI does not reimplement commit, DAG, index, cache, or remote business logic,
- `dml contrib status` is a pass-through status surface over `daggerml.contrib.status.status()`,
- errors are normalized into structured JSON payloads.

## Behavior Contracts

- default output is compact JSON,
- repo path resolution order is `--repo` -> `DML_REPO` -> cwd,
- remote root resolution order is `--remote-root` -> `DML_REMOTE_ROOT`,
- verbosity controls logging level only,
- expected domain errors MUST NOT emit unstructured tracebacks,
- `remote` commands use remote operation methods,
- `remote` and `cache` are separate command domains,
- `contrib status` emits the structured contrib status report as compact JSON,
- `cache` supports `list|get|put|delete|clear`,
- runtime config naming follows [configuration.md](configuration.md): `remote.root`.

## Encoding and Decoding

`DmlJsonEncoder` defines wire-format conventions for refs and key internal datums such as `Uri` and `Runnable`.

## Stability Notes

The CLI surface is operational and can evolve faster than the public Python API.

## References

- [configuration.md](configuration.md)
- [internal/ops/dml-ops.md](internal/ops/dml-ops.md)
