# Write And Test A Funk

This is the shortest path from a Python function to a contrib-backed DAG call.

## 1. Start with a script-backed function

```python
from daggerml.contrib import api

@api.funkify(uri="script", adapter="local")
def hello(dag, arg):
    from uuid import uuid4

    return f"{uuid4() = !s} and {arg.value() = }."
```

This is the same pattern used in `examples/00-hello_world.py`.

## 2. Call it from a DAG

```python
import daggerml as dml

with dml.new(name="examples/00-hello-world") as dag:
    dag.hello_fn = hello
    result = dag.call(hello, 23, name="greeting")
    dag.commit(result)
```

`arg` is node-like at runtime, so contrib author code usually reads inputs through `.value()`.

## 3. Keep script workers self-contained

The script worker only sees the serialized function source and anything you inject through `extra_objs` or `extra_lines`.

Good pattern:

- import dependencies inside the function body,
- or pass helper definitions through `extra_objs`,
- or inject source lines explicitly.

Risky pattern:

- relying on a module-level import or global constant that exists only in the author process.

## 4. Unit test the innermost callable with `defunkify`

```python
from daggerml.contrib.testing import defunkify

call = defunkify(hello)
assert call(None, 23) is not None
```

`defunkify(...)` walks to the innermost script runnable, returns the original callable, wraps non-leading arguments as node-like values, and runs the test in an isolated temporary working directory.

## 5. Use `MockNode` when you want explicit node-like values

```python
from daggerml.contrib.testing import MockNode

assert call(None, MockNode(23)) is not None
```

`MockNode` is intentionally small. It only gives you `.value()`. If a test needs real DAG, ref, or persistence behavior, switch to repository-backed APIs instead.

## 6. Reach for `dagclass` when the workflow has named members

Use `@api.dagclass` when you want a reusable DAG recipe with internal references and a named entrypoint. `api.run(instance, ...)` will create the DAG, materialize members, call the entrypoint, and commit the result.

See also: [reference/python-api.md](../reference/python-api.md)
