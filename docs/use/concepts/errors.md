# Errors

DaggerML distinguishes a failed computation from a value. A successful DAG has a result node. A failed DAG instead has a terminal `Error` object, stored under an `error:*` ref. A DAG cannot have both.

`Error` records a message, origin, type, and stack trace. It is durable repository state for a failed computation, not a datum: errors cannot be staged as literals, collection contents, function inputs, or function results.

## Capturing errors in a DAG

Use a `Dag` as a context manager when an uncaught Python exception should become the DAG's terminal error:

```python
import daggerml.api as api

dag = api.new("analysis", dml=dml)
with dag:
    raise ValueError("input is incomplete")
```

The exception still propagates to the caller. The context manager also commits a durable base `Error`, so the failed DAG can be inspected later. Passing an `Error` directly to `dag.commit()` likewise records it as the terminal error rather than as a result value.

## Function failures

Each function call records a function DAG. If the function fails, that DAG records an error ref instead of a result node; the parent DAG retains the function-call node and any name assigned to it.

High-level node access remains fail-fast. Calling a function that fails, loading its named node, or materializing that node raises `api.NodeError`, an `Error` subclass with the original error fields and the failed `node_ref`:

```python
import daggerml.api as api

try:
    failed_node = dag["err-val"]
except api.NodeError as error:
    print(error.message)
    print(error.node_ref)
```

`NodeError` is transient API context. If it is committed, DaggerML persists only a new base `Error` with its message, origin, type, and stack.

## Inspecting failures

`NodeError.context()` returns the function DAG that recorded the failure. This makes its named intermediate nodes available for normal inspection:

```python
try:
    dag["err-val"]
except api.NodeError as error:
    failed_dag = error.context()
    print(failed_dag.keys())
```

For low-level inspection, `dml.dag.get_node(node_ref)` returns either the concrete node value or its stored `Error`. Use `dml.dag.get_error(error_ref)` when an error ref is already known, such as from `dml.dag.describe(function_dag_ref)["error"]`.

The equivalent CLI workflow is:

```bash
dml dag get-node-by-name PARENT_DAG_REF err-val
# Use the printed node ref below.
dml dag describe-node ERROR_NODE_REF
# Use the printed function DAG ref below.
dml dag describe FUNCTION_DAG_REF
# Use the error ref from the description below.
dml dag get-error ERROR_REF
```

## Repository errors

`DmlRepoError` covers invalid repository operations, configuration failures, and malformed state. Unlike a stored terminal error, it indicates that DaggerML could not complete the requested repository operation. See the [error reference](../reference/errors.md) for common messages and recovery actions.
