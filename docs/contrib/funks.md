---
status: specified
doc_type: spec
---

# Contrib Funks

## Authority

This document is authoritative for contrib-owned prebuilt funks provided by `daggerml.contrib.funks`.

This document owns:

- the contrib funks module surface,
- the currently defined contrib funk catalog,
- the `docker_build` delayed-runnable interface and result contract.

This document does not own generic `api.funkify` behavior, adapter/executor runtime behavior, or `S3Store` API semantics.

## Scope

In scope:

- `src/daggerml/contrib/funks.py`,
- the exported `docker_build` contrib funk,
- the declaration-time contract for `docker_build`,
- the execution result contract for `docker_build`.

Out of scope:

- generic delayed-runnable construction semantics,
- executor selection/lifecycle,
- Docker daemon or registry semantics beyond the `docker_build` surface,
- `S3Store` implementation details.

## Purpose

Define the first contrib-owned prebuilt funk surface without overloading `daggerml.contrib.api` or executor contracts.

## Glossary

- Contrib Funk: a prebuilt contrib-owned delayed-runnable value exported from `daggerml.contrib.funks`.
- Docker Build Funk: the Contrib Funk exported as `docker_build`.
- Build Context Tarball: a tar archive containing the Docker build context consumed by the Docker Build Funk.
- Build Flags: the ordered sequence of raw Docker CLI build flags forwarded by the Docker Build Funk.

## Contract

### Interfaces

- Location:
  - `daggerml.contrib.funks`
- Current Contrib Funk catalog:
  - `docker_build`
- `docker_build` MUST be exported as a contrib delayed-runnable value produced through `api.funkify(...)`.
- `docker_build` MUST remain defunkifiable through `daggerml.contrib.testing.defunkify(...)` to recover the underlying script callable for author-code unit tests.
- The effective invocation shape of `docker_build` is:
  - `docker_build(context_tarball, build_flags=(), repo=None)`
- The underlying Python callable signature MAY remain `def _docker_build(dag): ...`; argument binding is owned by the script-executor calling convention rather than by a richer Python signature.
- Accepted inputs:
  - `context_tarball`: REQUIRED; MUST identify a Build Context Tarball.
  - `build_flags`: OPTIONAL; MUST be an ordered sequence of raw Docker build flags; default is the empty sequence.
  - `repo`: OPTIONAL; MAY identify a repository destination for push-oriented build flows; default is `None`.
- `docker_build` MUST treat `build_flags` as ordered pass-through Docker build CLI flags rather than a structured contrib-owned flag schema.
- `docker_build` MUST return a `Uri` when execution succeeds.
- Successful `docker_build` execution MUST externalize the produced build artifact bytes and MUST NOT store those bytes in repository storage.
- Unknown invocation fields beyond `context_tarball`, `build_flags`, and `repo` MUST be rejected.

### Invariants

- `docker_build` MUST remain a Contrib Funk surface rather than a contrib executor.
- `docker_build` MUST be constructible through `api.funkify(...)` and therefore follow `DelayedRunnable` declaration semantics from `docs/contrib/api.md`.
- `docker_build` MUST preserve Build Flags order exactly as supplied by the caller.
- Successful `docker_build` execution MUST produce an externally stored artifact referenced by `Uri`.
- Contrib funks MUST be pure with respect to their data arguments: the same funk invoked with the same data arguments MUST produce the same values regardless of caller-local process state such as current working directory, directory contents, host, user, or similar ambient environment details.

### Error Semantics

- Invalid `context_tarball` input:
  - non-retryable until the input is corrected,
  - terminal for that invocation,
  - caller behavior: construct a valid Build Context Tarball and retry,
  - operator action: repair the producer of the build context artifact.
- Invalid `build_flags` input:
  - non-retryable until the input is corrected,
  - terminal for that invocation,
  - caller behavior: correct the supplied Build Flags sequence,
  - operator action: repair the caller generating Docker flags.
- Docker build failure:
  - retryability depends on the underlying Docker/environment failure,
  - terminal for that invocation,
  - caller behavior: surface the execution failure,
  - operator action: inspect Docker/tooling/environment state and supplied build inputs.
- Artifact publication failure:
  - retryability depends on the external storage failure,
  - terminal for that invocation,
  - caller behavior: surface the storage failure,
  - operator action: restore storage availability or configuration.

### Authority Handoffs

- Generic `api.funkify` and `DelayedRunnable` behavior are authoritative in [api.md](api.md).
- Contrib executor runtime selection and lifecycle are authoritative in [runtime-contract.md](runtime-contract.md) and [executor-catalog.md](executor-catalog.md).
- External `Uri` semantics are authoritative in [../storing-and-retrieving-external-data.md](../storing-and-retrieving-external-data.md).
- `S3Store` API semantics are authoritative in [s3-store.md](s3-store.md).

## Compatibility

- This document defines only the currently implemented Contrib Funk catalog entry `docker_build`.
- Adding new Contrib Funks requires updating this document.
- Changing the `docker_build` invocation shape away from `(context_tarball, build_flags=(), repo=None)` is a compatibility-relevant contract change.
- Replacing ordered raw Build Flags with a structured contrib-owned option schema is a compatibility-relevant contract change.
- Changing successful `docker_build` results away from `Uri` is a compatibility-relevant contract change.

## References

- [api.md](api.md)
- [runtime-contract.md](runtime-contract.md)
- [executor-catalog.md](executor-catalog.md)
- [s3-store.md](s3-store.md)
- [../storing-and-retrieving-external-data.md](../storing-and-retrieving-external-data.md)
