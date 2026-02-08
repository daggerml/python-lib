# Error Model

## Status

specified

## Authority

This document is authoritative for error taxonomy, error propagation, and stable error contracts.
If related docs conflict on this scope, this document is the source of truth.


## Purpose

The error model defines domain error values and operational error boundaries across API, ops, and CLI.

## Error Layers

### Domain Error Values (`Error`)

- Type: `daggerml._internal.types.Error` (also exported as `daggerml.Error`).
- Meaning: a function run completed and produced a domain error value.
- Behavior: this error is stored in DAG state (`dag.error`) and is raised to callers when function results are materialized.

### Repository Operational Errors (`DmlRepoError`)

- Type: `daggerml._internal.types.DmlRepoError`.
- Inheritance: `DmlRepoError` is a subclass of `Error`.
- Meaning: repository/ops contract failure (invalid refs, invalid DAG state, adapter/process failures, contract violations).
- Boundary: canonical operational error at `_internal` subsystem boundaries.

### Low-Level Database Errors (`DmlDb*`)

- Types: `DmlDbError` and subclasses in `daggerml._internal._db`.
- Meaning: low-level storage/runtime failures from native DB bindings.
- Intended handling: mostly internal; retryable cases are handled by `with_retry` around ops methods.

## Retry and Transaction Semantics

- `with_retry` retries full operations on:
  - `DmlDbMapFullError` (after resize),
  - `DmlDbEnvReopenedError` (environment repaired; transaction invalidated).
- Transaction wrapper behavior:
  - re-raises `Error` unchanged,
  - wraps unexpected exceptions as `DmlRepoError("Transaction failed: ...")`.

## Execution Error Contracts

These messages are contract-level for execution paths:

- `DmlRepoError("Unknown kwarg: <key>")`
- `DmlRepoError("Runnable sub cycle detected")`
- `DmlRepoError("Adapter call failed: <stderr>")`
- `DmlRepoError("Adapter output must be JSON")`
- `DmlRepoError("Adapter output schema invalid")`
- `DmlRepoError("Remote context required for adapter invocation")`
- `DmlRepoError("Provide exactly one of head or argv_ptr.")` when index creation input mode is invalid.
- `DmlRepoError("Remote context required for argv_ptr")` when argv-pointer index creation is requested without remote context.

Additional execution validation errors (e.g., missing argv node, invalid runnable first arg, invalid builtin shape) are also surfaced as `DmlRepoError`.

Contrib-runtime-specific error contracts are defined by contrib runtime docs, including [contrib/runtime-contract.md](contrib/runtime-contract.md) and [contrib/api.md](contrib/api.md).

## CLI Error Surface

- CLI normalizes failures into structured JSON error payloads.
- Expected domain failures should not emit raw tracebacks in normal CLI output.

## Scope

This document defines the scope described by its authority and purpose sections.

## Content

See the sections in this document for normative content.

## References

None.
