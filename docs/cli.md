# CLI Design Contracts (`daggerml._cli`)

## Status

specified

## Authority

This document is authoritative for CLI subsystem contracts.

## Purpose

Define the minimal command wrapper around `DmlOps` with automatic configuration resolution and JSON output and error handling.

## Scope

CLI is an operational interface over `_internal` ops. It owns argument parsing and output normalization; shared configuration precedence, validation, and derivation are delegated to `daggerml._internal.config.DmlConfig`.

## Routing Model

- top-level `dml` dispatches by operation group,
- subcommands call corresponding `DmlOps` methods via thin handlers,
- the CLI does not reimplement commit, DAG, index, cache, or remote business logic,
- `dml contrib status` is a pass-through status surface over `daggerml.contrib.status.status()`,
- errors are normalized into structured JSON payloads.

## Behavior Contracts

- default output is compact JSON,
- repo-path resolution is delegated to the shared internal resolver,
- remote project-root resolution is delegated to the shared internal resolver,
- verbosity controls logging level only,
- expected domain errors MUST NOT emit unstructured tracebacks,
- `remote` commands use remote operation methods,
- `remote` and `cache` are separate command domains,
- top-level git-like project commands include `clone`, `checkout`, `fetch`, `pull`, `push`, `merge`, and `revert`, with remote subcommand equivalents for lower-level S3 sync operations,
- `checkout <revision>` resolves revisions locally (branch, tag, commit ref, or ancestry expression), reports attached vs detached mode explicitly, and does not perform implicit network fetches,
- `clone` composes `fetch` then `checkout`; branch targets attach, tag/commit-expression targets detach, direct commit-target clone is rejected until fetch supports commit retrieval, and clone initialization does not invoke `init` hooks,
- `dag checkout <revision> <dag-name> [--as <name>] [--replace]` copies one DAG from history into the current branch as a new commit,
- `contrib status` emits the structured contrib status report as compact JSON,
- `cache` supports `list|get|put|delete|clear`,
- runtime config naming follows [configuration.md](configuration.md): `project.home`, `project.uri`, `db.path`, `remote.uri`, `user`, `default_branch`, `hooks.post-init`, `hooks.post-clone`, and `config_home`.

## Serialization-Limited Gaps

- The CLI does not expose API-only flows that depend on Python object or function serialization.
- There is no CLI equivalent for `@api.funkify`, passing in-process Python callables, or staging arbitrary live Python objects as execution inputs.
- Features built around decorator-driven runtime helpers or direct in-process object transport remain API-only by design.
- Supported CLI inputs are the explicit command arguments the parser can serialize directly, such as strings, numbers, booleans, `namespace:id` ref strings, and other JSON-serializable values accepted by that command.

## Encoding and Decoding

`DmlJsonEncoder` defines wire-format conventions for refs and key internal datums such as `Uri` and `Runnable`.

## Stability Notes

The CLI surface is operational and can evolve faster than the public Python API.

## References

- [configuration.md](configuration.md)
- [internal/ops/dml-ops.md](internal/ops/dml-ops.md)
