# CLI Design Contracts (`daggerml._cli`)

## Status

specified

## Authority

This document is authoritative for CLI subsystem contracts.

## Purpose

Define the thin command wrapper around `daggerml._internal.Dml` with automatic configuration resolution plus JSON and plain-text output handling.

## Scope

CLI is the public operational interface over the shared internal `Dml` boundary. It owns argument parsing and output normalization; repository orchestration, revision resolution, DAG lookup, and admin workflows are delegated to `daggerml._internal.Dml`.

## Routing Model

- top-level `dml` dispatches by public porcelain verb or namespace,
- handlers call public `Dml` methods and namespaces only,
- CLI handlers do not reach into `Dml.ops` or private `_...ops()` helpers,
- repository inspection verbs are `status`, `show`, `log`, `diff`, `checkout`, `branch`, `fetch`, `pull`, `push`, `merge`, and `revert`,
- DAG workflows live under `dml dag`,
- maintenance workflows live under `dml admin`,
- `dml config show [--contrib]` is the JSON config-status entrypoint,
- errors are normalized into structured JSON payloads.

## Behavior Contracts

- default output is compact JSON,
- `config get` and `config set` are the plain-text exceptions to default JSON output,
- repo-path resolution is delegated to the shared internal resolver,
- remote project-root resolution is delegated to the shared internal resolver,
- verbosity controls logging level only,
- expected domain errors MUST NOT emit unstructured tracebacks,
- top-level git-like project commands include `status`, `show`, `log`, `diff`, `checkout`, `branch`, `fetch`, `pull`, `push`, `merge`, and `revert`,
- `checkout <revision>` resolves revisions locally (branch, tag, commit ref, or ancestry expression), reports attached vs detached mode explicitly, and does not perform implicit network fetches,
- `status` reports repository state as JSON with `head`, `branches`, `dags`, and `indexes`,
- `show <revision>` returns JSON with top-level `revision`, `commit`, `dags`, and `change`,
- `log [<revision>] [--limit N]` returns JSON with `revision` and `commits`,
- `diff [<left>] [<right>]` returns JSON with `left`, `right`, `added`, `removed`, and `updated`,
- `branch` lists local branches and `branch --remote` lists remote-tracking branch selectors,
- `dag list [--revision REV]` returns a revision-scoped DAG map,
- `dag get <name-or-id> [--revision REV]` resolves by DAG name within a revision or exact `dag:<id>` selector and returns node data in the `dag` payload,
- `dag checkout <revision> <dag-name> [--as <name>] [--replace]` copies one DAG from history into the current branch as a new commit,
- `dag delete <name>` removes one named DAG from a branch and commits the change,
- `admin index list|get|delete` exposes live-index inspection and deletion,
- `admin cache invalidate <cache-key>...` accepts exact cache keys only,
- `admin remote list [--owner OWNER]` lists canonical project URIs and `admin remote list dml://<owner>/<project>` lists that project's tracked branches and tags,
- `admin remote gc` runs remote maintenance,
- `admin gc [--dry-run]` runs or previews local garbage collection,
- `init` accepts optional `--remote-root` and optional `--remote-project`,
- `init --remote-project` requires `--remote-root`,
- `init` performs project fetch/bootstrap only when `--remote-project` is configured,
- `push`, `pull`, and `fetch` require configured `remote.project`; `remote.root` alone is not sufficient for project sync,

Operational note:
- Do not run local `admin gc` concurrently with `runtime.cancel(...)`. Cancellation reuses locally materialized adapter input state while it walks the rooted execution set.
- runtime config naming follows [configuration.md](configuration.md): `project.home`, `remote.project`, `db.path`, `remote.root`, `remote.fetch_workers`, `user`, `default_branch`, `hooks.post-init`, and `config_home`.
- explicit CLI override flags mirror the canonical config naming, including `--project-home`, `--remote-root`, `--remote-project`, and `--config-home`.

## Serialization-Limited Gaps

- The CLI does not expose API-only flows that depend on Python object or function serialization.
- There is no CLI equivalent for `@api.funkify`, passing in-process Python callables, or staging arbitrary live Python objects as execution inputs.
- Features built around decorator-driven runtime helpers or direct in-process object transport remain API-only by design.
- Supported CLI inputs are the explicit command arguments the parser can serialize directly, such as strings, numbers, booleans, `namespace:id` ref strings, and other JSON-serializable values accepted by that command.

## Encoding and Decoding

`DmlJsonEncoder` defines wire-format conventions for refs and key internal datums such as `Uri` and `Runnable`.

## Stability Notes

The CLI surface is intentionally breaking in this redesign and no legacy aliases or legacy public command groups are preserved.

## References

- [configuration.md](configuration.md)
- [internal/ops/README.md](internal/ops/README.md)
