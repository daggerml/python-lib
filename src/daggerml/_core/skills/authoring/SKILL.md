---
name: daggerml-authoring
description: Build reproducible DaggerML DAGs and script-backed funks.
---

# DaggerML Authoring

Use the Python API to build computation graphs; use `dml` commands to initialize
projects and inspect repository state. Work in an initialized project (`dml
init`); configure `remote.root` before remote-backed script execution and cache
coordination. Keep `.dml/` managed by DaggerML tooling.

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

The first call receives the `number` node, not its Python value; the second
receives the `direct` call result node. The worker materializes each input only
when performing arithmetic. If a call fails during authoring, the named call
and its execution DAG remain inspectable even if you catch the error and commit
another result.

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

For an existing result, prefer `input_node = dag.require("upstream",
name="input")` and pass `input_node` into downstream calls. When you need a
specific named intermediate, use `dag.require("upstream", "intermediate")`.
To reuse only part of a committed collection, select it before staging:
`dag.put(source["records"][0], name="first")`. This keeps the committed
base and selection path rather than copying a materialized value.

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

For example, a worker that needs a library can import it in the body; a
source-defined helper must be included explicitly:

```python
def clean(value):
    return value.strip().lower()


@api.funkify(extra_objs=(clean,))
def normalize(dag, text):
    return clean(text.value())


with dml.new("cleaned", message="normalize a label") as dag:
    raw = dag.put("  EXAMPLE  ", name="raw")
    cleaned = dag.call(normalize, raw, name="cleaned")
    dag.commit(cleaned)
```

The generated script contains `clean`; an unlisted module global or closure
would not be available to this worker. Use `post_lines` for explicit module
definitions when appropriate. `defunkify()` can test plain funk logic quickly,
but only a real call checks worker isolation, storage, and cache behavior.

## Compose A Dagclass

Use a dagclass when several named steps and parameters form a reusable
pipeline. Open `examples/dagclass.py` in this skill directory for a complete
airline-delay regression example. It stages the public Vega flights sample as
an artifact, splits it deterministically, and returns Polars DataFrames so the
installed codec persists each cut as Parquet. Later Docker funks read the
Parquet URIs directly with Polars, train and pickle decision trees, predict
both cuts, compute R²/MSE/MAE, and select the best out-of-sample R².
The `search` method retains every `{params, objective}` as the named `trials`
node in its execution DAG. `main` prints the winning parameters and score and
returns the best trial. Run from an initialized project with `remote.root`
configured: `python path/to/daggerml-authoring/examples/dagclass.py IMAGE`,
where IMAGE contains DaggerML, polars, and scikit-learn. The example's `run()`
also accepts a staged dataset URI and Docker flags for other environments.
For S3-compatible endpoints, pass `AWS_ENDPOINT_URL` into the Docker worker;
the script explicitly gives that endpoint to Polars's cloud reader.

Annotated fields are constructor inputs; direct `self.member` reads let the
compiler discover method dependencies. `api.run()` stages class members, calls
`main`, and commits its result. In each method's worker, `self` is a DAG:
`self.trials` creates an invocation-local node, not an instance attribute shared
with later calls. Keep the model and dataset as artifact URIs and pass graph
nodes between methods; call `.value()` only to train, compute metrics, iterate
the parameter sets, or choose and print the best result. Each script method is
isolated from module-level imports and globals.

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

## Check An Authored Result

After committing, reload the DAG and compare `result` with any named nodes you
intend to expose. Follow a returned call node with `.context(root=False)` to
inspect its immediate execution DAG; repeated calls with the same runnable and
normalized inputs can have distinct authoring nodes but share that execution
DAG. If work unexpectedly reused a result, check the rendered script and its
explicit helper source, staged runnable configuration, and actual node inputs.
Changing unrelated authoring code does not necessarily change cache identity.
