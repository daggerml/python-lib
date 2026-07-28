# Author a DAG

Authoring starts in the user's runtime: the mutable DAG returned by
`dml.new(...)`. Stage values and functions there, call them to record work, and
commit the final node.

## Use the runtime

Initialize the project first with `dml init`, then create a runtime in an
authoring script:

```python
import daggerml as dml


with dml.new("analysis", message="summarize inputs") as dag:
    raw = dag.put([1, 2, 3], name="raw")
    summary = dag.put({"count": len(raw), "values": raw}, name="summary")

    assert dag["raw"] == raw
    assert dag.summary == summary
    dag.commit(summary)

print(dml.load("analysis").result.value())
```

`Dag.put()` stages a value and returns its node. Naming a node makes it
available through `dag["name"]` or `dag.name`; the name is a label, not a copy.
Call `.value()` to materialize a node's value. Use
`dag.require("other-dag")` to import the committed result of another DAG.

## Author a funk

Use `@api.funkify` to package a Python function as a runnable funk:

```python
import daggerml as dml
from daggerml.contrib import api


@api.funkify
def square(dag, number):
    return number.value() ** 2


with dml.new("squares") as dag:
    _ = dag.call(square, 9, name="foo")
	# or alternatively
	dag.fn = square
	result = dag.fn(3, name="bar")
    dag.commit(result)
```

Funk arguments are node-like in the worker, so read input with `.value()`.
Calling a staged funk records a function-call node. Script-backed funks execute
in a separate worker and receive the function source, not module globals. Import
dependencies inside the function body or inject them explicitly.

Use `extra_objs` to include helper definitions in the generated script, or
`post_lines` to append source lines after those definitions:

```python
def clamp(value):
    return max(0, min(value, 1))


@api.funkify(extra_objs=(clamp,))
def normalize(dag, number):
    return clamp(number.value() / 100)
```

Script-backed execution uses remote artifacts; configure `remote.root` before running it. Test author code with `daggerml.contrib.testing.defunkify` when a full runtime is unnecessary.

## Complex and nested funks

Consider a pipeline where `main` calls `preprocess`, and `preprocess` calls `parse_numbers` and `normalize`. With only `@api.funkify` as we've shown it, every function must receive the funks it calls:

```python
@api.funkify
def parse_numbers(dag, raw):
    return [int(value) for value in raw.value()]


@api.funkify
def normalize(dag, values):
    values = values.value()
    return [value / max(values) for value in values]


@api.funkify
def summarize(dag, values):
    values = values.value()
    return {"count": len(values), "total": sum(values)}


@api.funkify
def preprocess(dag, raw, parse_numbers_fn, normalize_fn):
    return normalize_fn(parse_numbers_fn(raw))


@api.funkify
def main(dag, raw, preprocess_fn, summarize_fn, parse_numbers_fn, normalize_fn):
    values = preprocess_fn(raw, parse_numbers_fn, normalize_fn)
    return summarize_fn(values)
```

The forwarding is necessary. A script funk can only call the values passed to it, and the cached `main` call must include the functions and arguments of every nested call. Omitting one can make different computations share a cache identity and reuse the wrong result.

## Prepopulate named nodes

`api.ref("name")` defers a named-node lookup. When its containing delayed value
is staged, it resolves as `dag["name"]`. This moves a dependency from the
function signature to DAG staging order: `name` must already be in the DAG when
the delayed value is inserted.

The script executor's `prepop` argument stages values under local names in the
worker DAG before the script runs:

```python
@api.funkify(
    prepop={
        "parse_fn": api.ref("parse_numbers"),
        "norm_fn": api.ref("normalize"),
    }
)
def preprocess(dag, raw):
    return dag.norm_fn(dag.parse_fn(raw))


@api.funkify(
    prepop={
        "proc_fn": api.ref("preprocess"),
        "sum_fn": api.ref("summarize"),
    }
)
def main(dag, raw):
    return dag.sum_fn(dag.proc_fn(raw))


with dml.new("dataset-summary") as dag:
    dag.parse_numbers = parse_numbers
    dag.normalize = normalize
    dag.summarize = summarize
    dag.preprocess = preprocess
    dag.main = main
    dag.commit(dag.main(["2", "4", "8"]))
```

The functions now accept only the data they operate on. Staging `preprocess`
resolves its `parse_numbers` and `normalize` references, so those nodes must
already exist. Similarly, stage `preprocess` and `summarize` before `main`.

`prepop` is stored in the staged runnable. The runnable for `main` contains the
`preprocess` and `summarize` nodes; `preprocess` contains its own child nodes.
The cache identity of a `main` call therefore follows this complete dependency
graph, including descendant functions and their configuration.

## Use a dagclass

When the named declarations and `dag[...]` lookups become repetitive, use
`@api.dagclass` to express the same composition with a Python class:

```python
@api.dagclass
class DatasetSummary:
    parse_numbers = parse_numbers
    normalize = normalize
    summarize = summarize

    def preprocess(self, raw):
        return self.normalize(self.parse_numbers(raw))

    def main(self, raw):
        return self.summarize(self.preprocess(raw))


api.run(DatasetSummary(), ["2", "4", "8"], name="dataset-summary")
```

- `@api.dagclass` collects named members and finds `self.<member>` dependencies.
- Dagclass methods use the same script `funkify` and `prepop` machinery as an
  explicit function.
- `api.run()` creates a DAG, stages members in dependency order, calls the
  entrypoint method (`main` by default), and commits its result.

Dagclass is a thin convenience wrapper, not a source normalizer. A dagclass
method and an explicit `@api.funkify(prepop=...)` function share a cache when
their user-authored function, configuration, and arguments are the same.

Dagclasses can also be composed as reusable functions:

```python
@api.dagclass
class MultiDatasetSummary:
    summarizer = DatasetSummary()

    def main(self, raw_dict):
        return {name: self.summarizer(raw) for name, raw in raw_dict.items()}


api.run(MultiDatasetSummary(), {"a": ["2", "4", "8"], "b": ["1", "3", "5"]})
```

The nested instance is a self-contained runnable: the parent DAG stages only
`summarizer`, while its internal member graph remains part of that runnable's
cache identity.

`api.load("dag-name")` is another delayed authoring value. When staged, it
imports the committed result of `dag-name`, equivalent to `dag.require("dag-name")`.

See `examples/python/03-dagclass.py` for a runnable dagclass pipeline.
