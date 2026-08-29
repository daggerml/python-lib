---
name: daggerml-authoring
description: Build reproducible DaggerML DAGs and script-backed funks.
---

# DaggerML Authoring

## Create And Commit A DAG

`dml.new(name, message=...)` creates a mutable DAG. Stage data and functions,
record calls, then explicitly commit one node as the terminal result. Successful
exit from a `with` block does not commit automatically. A committed DAG is
immutable; load it with `dml.load(name)` and read its terminal node through
`.result`.

## Put And Get Data

`dag.put(value, name=...)` stages a value and returns its node. Assignment is a
short form for staging named data: `dag.foo = value` and `dag["foo"] = value`.
Use item syntax for names that collide with `Dag` attributes or methods.

Read named nodes with `dag.foo` or `dag["foo"]`; inspect available names with
`dag.keys()` and their nodes with `dag.values()`. A name is a label, not a copy.
`dag.result` is available only on a committed DAG and means its terminal node; a
node named `"result"` is just `dag["result"]`.

Keep nodes in the graph. Nodes can be staged inside lists or dictionaries,
passed to functions, or indexed. Call `.value()` only for inspection or when
ordinary Python code needs concrete data. Collection-valued nodes support normal
key, index, and slice access: `dag.foo["bar"]` returns another node in the same
DAG, and `dag.foo["bar"].value() == dag.foo.value()["bar"]`. Pass the selected
node directly into calls or other collections to avoid materializing the parent.

```python
import daggerml as dml
from daggerml.contrib import api


@api.funkify
def square(dag, number):
    return number.value() ** 2


with dml.new("squares", message="square an input") as dag:
    number = dag.put(3, name="number")
    dag["metadata"] = {"unit": "meters"}

    assert dag.number == number
    assert dag["metadata"].value() == {"unit": "meters"}

    direct = dag.call(square, number, name="direct")
    dag.square = square
    result = dag.square(direct, name="squared-again")
    dag.commit(result)

assert dml.load("squares").result.value() == 81
```

## Call Functions

Call a funk directly with `dag.call(fn, *args, name=...)`, or stage it as a node
and call that node: `dag.fn = fn; dag.fn(*args, name=...)`. `dag.put(fn,
name="fn")` is the explicit equivalent of assignment. In both forms, arguments
may be literals, nodes, projections, or nested collections containing them, and
the returned node records the call result. The call's `name=` labels that result;
it does not name the function.

Pass node-like arguments unchanged to nested funks. Do not call `.value()`
between graph calls: that materializes and restages a copy instead of preserving
the dependency edge. Write calls in their logical order without trying to stage,
schedule, or deduplicate them: DaggerML ensures a given funk and normalized
arguments run only once and reuses the cached result thereafter.

## Load Nodes From Other DAGs

There are two general forms: `dag.require(...)`, or load a committed DAG and
stage one of its nodes with `dag.put(...)`, item assignment, or attribute
assignment.

With `dag.require(dag_name, name=...)`, the source DAG's terminal result is
imported. `dag.require(dag_name, node_name, name=...)` always imports the named
node. Therefore `dag.require("other-dag", "result")` imports
`dml.load("other-dag")["result"]`, not `dml.load("other-dag").result`.
`name=` labels the imported node in the target DAG; it does not select or rename
the source node.

For `source = dml.load("other-dag")`, `dag.require("other-dag")` is equivalent
to `dag.put(source.result)`. `dag.require("other-dag", "bar", name="foo")` is
equivalent to `dag.put(source["bar"], name="foo")` or
`dag.foo = source["bar"]`.

`dag.require(source)` also accepts a loaded committed `Dag`; its second argument
selects a named node. This form preserves an explicit revision, fetched remote,
or dependency selected when loading `source`. The loaded DAG must belong to the
target DAG's `Dml` session. Importing an uncommitted DAG or a node from another
open runtime fails.

## Author Funks

`@api.funkify` packages delayed work. Worker arguments are node-like: materialize
with `.value()` for arithmetic, iteration, or library calls, but pass them
unchanged to nested funks. Other funks must be explicit through arguments,
`prepop`, or dagclass members.

Script workers receive rendered function source plus `extra_objs` and
`post_lines`, not module globals, closures, module imports, constants, or
transitive helpers. Import dependencies inside the function and inject all
behavior-affecting helper source. `prepop` creates named nodes on the worker DAG;
`api.ref("name")` resolves configuration from an already-named authoring node.
`logger` is injected.

## Compose A Dagclass

```python
@api.funkify
def add(dag, left, right):
    return left.value() + right.value()


@api.dagclass
class Pipeline:
    add = add
    offset = 23

    def prepare(self, value):
        self.foo = 23
        return self.add(value, self.foo)

    def main(self, value):
        self.prepare(value)
        return self.add(value, self.offset)


assert type(Pipeline().main) is type(add)
api.run(Pipeline(), 19, name="answer")
```

A dagclass is compiled composition syntax, not a normal stateful class. The
compiler resolves direct `self.add`, `self.offset`, and `self.prepare`
references to declared members and packages each method as an isolated funk.
When `prepare` executes, `self` is that invocation's worker `Dag`; setting
`self.foo = 23` creates a node only there. It does not mutate the `Pipeline`
instance and has no effect on `main` or any later method execution. Referencing
`self.foo` from `main` would therefore require a declared class member named
`foo`; it cannot observe the assignment performed by `prepare`.

## Manage Cache Identity

Cache reuse keys on the staged runnable and normalized DaggerML input identity.
Chunk expensive input work when independently reusable chunk results will avoid
recomputing the whole dataset, but do not create funks without a meaningful
reuse boundary. A leaf funk should accept only arguments it uses: unrelated
arguments cause cache misses unless intentionally supplied as a cache breaker.
Editable imported package code is not automatically part of that identity, so
pin environments and package changing helpers. Put supported complex values
directly and let installed codecs normalize them; the included pandas and polars
DataFrame codecs persist Parquet artifacts automatically. For other files,
directories, bytes, or JSON artifacts, store them with
`daggerml.contrib.s3.S3Store` and put the returned `Uri` in the DAG.
