---
name: daggerml-authoring
description: Build reproducible DaggerML DAGs and script-backed funks.
---

# DaggerML Authoring

Use Python to author computations. A `Dag` is mutable until `commit()`, then
immutable and inspectable.

## Author DAGs

Use `dag.put()` for storable values, `dag.call()` for funks, and
`dag.require("other-dag")` for a prior committed result. Name inspectable values
and calls, then commit the terminal node. Materialize nodes with `.value()`.
Pass same-session nodes, projections, and required DAG results directly: the
codec preserves imports and access paths.

```python
import daggerml as dml
from daggerml.contrib import api


@api.funkify
def summarize(dag, numbers, divisor):
    logger.info("Summarizing %s", numbers.value())  # injected logger
    print(type(dag))  # `daggerml.api.Dag` instance
    print(numbers[0].value())  # indexed graph node
    print(numbers[1:].value())  # list nodes support slices
    sum_ = dag.put(sum(numbers.value()), name="sum")
    return sum_.value() / divisor.value()

with dml.new("summary") as dag:
    values = dag.put([2, 3, 5], name="values")
    result = dag.call(summarize, values, 2, name="summary")
    print(result.context().sum.value())  # 10
    dag.commit(result)

dml.load("summary").summary.value()  # 5.0
dml.load("summary").summary.context().result.value()  # 5.0
```

## Author Funks

`@api.funkify` packages delayed work. Its worker receives node-like arguments,
so materialize inputs before using them. Script workers receive only function
source plus injected `extra_objs` or `post_lines`, not module globals. Import
dependencies inside the funk, and package behavior-affecting helpers. `logger`
is injected for logging.

## Sharp Bits

`remote.root` is required for adapter-backed execution and cache coordination.
Cache reuse keys on the staged runnable and normalized DaggerML input identity.
Include all behavior in the packaged function boundary; otherwise helper edits
can silently reuse an old result.

For implementation detail, inspect installed `daggerml.api`,
`daggerml.contrib.api`, `daggerml._core.index`, and
`daggerml._core.exec_state`.
