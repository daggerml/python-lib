---
name: daggerml-authoring
description: Build reproducible DaggerML DAGs and script-backed funks.
---

# DaggerML Authoring

Use Python to author computations. A `Dag` is mutable while authored and
becomes an immutable, inspectable result after `commit()`.

## Author DAGs

Use `dag.put()` for storable values, `dag.call()` for funks, and
`dag.require("other-dag")` for a prior committed result. Name values and calls
whose provenance must be easy to inspect. Nodes materialize with `.value()`.
Prefer same-session nodes and required DAG results to materialized copies: the
codec preserves imports and projection access paths in the new graph.

```python
import daggerml as dml

with dml.new("summary") as dag:
    values = dag.put([2, 3, 5], name="values")
    result = dag.call(summarize, values, name="summary")
    dag.commit(result)
```

## Author Funks

`@api.funkify` packages delayed work. Its worker receives node-like arguments,
so materialize inputs before using them:

```python
from daggerml.contrib import api

@api.funkify
def square(dag, number):
    return number.value() ** 2
```

Script workers receive function source, not module globals. Import dependencies
inside the funk. Inject inspectable helpers with `extra_objs` and required
source lines with `post_lines`; module-level imports and editable helper code
are otherwise unavailable to the worker.

## Sharp Bits

`remote.root` is required for adapter-backed execution and cache coordination.
Cache reuse keys on the staged runnable and normalized DaggerML input identity.
Include helper source in the packaged function boundary so helper changes cannot
silently reuse an old result.

For implementation detail, inspect installed `daggerml.api`,
`daggerml.contrib.api`, `daggerml._core.index`, and
`daggerml._core.exec_state`.
