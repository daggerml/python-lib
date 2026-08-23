---
name: daggerml
description: Concise guidance for coding agents working with DaggerML projects.
---

# DaggerML

Use the virtual environment with `daggerml`. Use the CLI for project, history, remote, runtime, and administration work; use Python to author computations. Start with `dml --help`, docstrings, and examples.

```bash
dml init
dml status
python analysis.py
dml show
dml log
```

## Author DAGs

A `Dag` is mutable while authored. It records values and function calls as nodes; `dag.commit()` makes the final node durable and `dml.load()` reopens it.

```python
import daggerml as dml

with dml.new("summary") as dag:
    values = dag.put([2, 3, 5], name="values")
    dag.commit(values)

assert dml.load("summary").result.value() == [2, 3, 5]
```

Use `dag.put()` for storable values, `dag.call()` for funks, and `dag.require("other-dag")` for a prior committed result. Nodes materialize with `.value()`.

## Author Funks

`@api.funkify` packages delayed work. Its worker receives node-like arguments, so materialize inputs before using their values:

```python
from daggerml.contrib import api

@api.funkify
def square(dag, number):
    return number.value() ** 2
```

Script workers receive function source, not module globals. Import dependencies inside the funk, or inject inspectable helpers with `extra_objs` and source lines with `post_lines`. Remote-backed execution and cache coordination require `remote.root`.

```python
# Wrong: this module-level import is unavailable in the script worker.
import numpy as np

@api.funkify
def mean(dag, values):
    return np.mean(values.value())

# Right: the funk source imports NumPy in its worker.
@api.funkify
def mean(dag, values):
    import numpy as np
    return np.mean(values.value())
```

## Compose DAGs

Use `@api.dagclass` to name a funk graph. Dagclasses compose: a member dagclass instance is a reusable runnable in its parent.

```python
@api.funkify
def summarize(dag, values):
    return {"count": len(values.value())}

@api.dagclass
class DatasetSummary:
    summarize = summarize

    def main(self, raw):
        return self.summarize(raw)

@api.dagclass
class MultiDatasetSummary:
    summarizer = DatasetSummary()

    def main(self, raw_dict):
        return {name: self.summarizer(raw) for name, raw in raw_dict.items()}
```

## Sharp Bits

`remote.root` is required before creating, staging, committing, or executing a
DAG. Do not expect an unconfigured repository to accept writes.

Imported editable helper code is not part of a script funk's cache key. Include
helper source so a helper change cannot silently reuse an old result:

```python
@api.funkify(extra_objs=(normalize,))
def score(dag, values):
    return normalize(values.value())
```

Never run administrative work while pulling or synchronizing the same project:

```bash
# Do not run these concurrently.
dml pull
dml gc
```

## Boundaries

Use DaggerML tooling rather than manually modifying `.dml/` managed objects or refs. Inspect failures through node/error context and `dml dag` commands.
