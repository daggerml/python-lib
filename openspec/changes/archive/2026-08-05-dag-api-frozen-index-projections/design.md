## Context

Runtime already replaces an active user index with a `frozenindex:` ref and preserves its partial DAG reference. The API wrapper retains a `token` for every uncommitted runtime, but its named-node lookup currently calls `runtime.get_node` directly rather than using the durable DAG projection path.

## Decisions

### Lifecycle methods mutate only the wrapper token

`Dag.freeze(message=None)` calls `dml.runtime.freeze(self._require_index_ref(), message=...)`, passing `dag: <Dag.name>` plus a newline and the non-empty caller annotation when present. It stores the returned frozen ref in `self.token`, and returns `self`. `Dag.unfreeze()` does the symmetric operation. Neither method is available for a committed DAG because it has no runtime index token.

### Project frozen indexes as partial DAGs

A private helper resolves the backing DAG ref as `self.ref` for completed DAGs or `dml.runtime.describe(self._require_index_ref())["dag"]` for active and frozen indexes. Named node lookup, `keys`, `values`, and `argv` inspect that ref with `dml.dag.describe(...)`.

This deliberately does not set `Dag.ref` for a frozen index: it remains uncommitted, so `.result` must continue to reject it.

### Preserve immutability

No method automatically unfreezes or refreezes. Existing calls to runtime mutation methods continue to receive the frozen token and therefore retain the core freeze invariant.

## Non-goals

- No changes under `src/daggerml/_core/`.
- No behavior change to committed DAGs.
- No new serialized data or runtime operation.
