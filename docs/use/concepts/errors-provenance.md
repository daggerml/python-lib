# Errors and provenance

Repository, configuration, runtime, and execution failures normally surface as `DmlRepoError`. A `daggerml.Error` can also be a stored DAG value: using a `Dag` as a context manager captures an uncaught exception and commits it as the result.

Every result can be traced through imports and function calls. `node.context()` returns the DAG that produced a node or projected subvalue; `root=True` follows provenance across boundaries to its root context.

Use `dml show`, runtime graph inspection, and [inspect failures](../guides/inspect-failures.md) before retrying. See the [error reference](../reference/errors.md) for common messages.
