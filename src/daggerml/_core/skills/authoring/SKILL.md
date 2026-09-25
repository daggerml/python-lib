---
name: daggerml-authoring
description: Use when writing or reviewing DaggerML code. Applies DML-specific graph, caching, provenance, and execution conventions.
---

# DaggerML Authoring

Treat the DaggerML docs and current source as the authority for API behavior.

Use this skill to decide **how DML code should be structured**, not to replace the API reference.

For runnable examples, see the `examples/` directory in this skill; they're all tested against this version of dml.

## Preserve The Graph

Keep values represented as DML nodes or projections for as long as they remain part of the computation graph.

Pass nodes, projections, and collections containing them directly between DML operations.

Do not call `.value()` merely to pass a result into another funk or DAG operation. Materializing and restaging a value discards graph structure that DML could otherwise preserve.

Materialize only when ordinary Python execution actually requires the concrete value, such as:

- arithmetic inside a worker;
- iteration required by a library;
- passing data to an external Python API;
- explicit inspection or debugging.

When reusing existing DML state, import or stage the existing node rather than its materialized Python value.

Prefer:

```python
features = dag.require("features")
prediction = dag.call(model, features)
```

over:

```python
features = dml.load("features").result.value()
prediction = dag.call(model, dag.put(features))
```

When only part of a collection is needed, preserve the projection when possible rather than materializing the parent collection first.

## Make Dependencies Explicit

If changing some state can change a computation's result, represent that state explicitly in DML.

Behavior-affecting state should normally appear in one of:

- the runnable itself;
- funk arguments;
- staged configuration;
- explicitly injected worker source;
- explicit DML dependencies.

Do not silently depend on mutable ambient Python state.

Prefer explicit graph dependencies over hidden filesystem state, module globals, closures, process state, or implicit configuration.

This is especially important when the value should affect provenance or cache identity.

## Choose Funk Boundaries Deliberately

Treat a funk as an **execution, caching, provenance, and potentially runtime boundary**, not merely as a Python function.

Create a separate funk when the computation has a meaningful independent execution boundary, for example when it:

- is expensive;
- is independently reusable;
- benefits from independent caching;
- should be separately inspectable;
- should run under a different executor or environment;
- represents a meaningful persisted stage of a computation.

Do not turn every Python helper into a funk.

Use ordinary Python functions for local implementation structure when they do not need independent DML semantics.

Prefer coarse enough funks to avoid meaningless graph fragmentation, but fine enough funks that expensive reusable results can be cached independently.

## Keep Funk Inputs Minimal

A leaf funk should accept the inputs that actually determine its behavior.

Do not thread unrelated configuration or large context objects through every funk for convenience.

Unrelated arguments can unnecessarily distinguish otherwise identical executions and reduce cache reuse.

If an argument is intentionally included only to invalidate cache identity, make that intent clear.

## Treat Script Workers As Isolated Programs

Reason about every script-executed funk as though its function body were copied into a fresh Python module.

Do not assume access to:

- module-level imports;
- module globals;
- constants defined elsewhere in the file;
- closures;
- neighboring helper functions;
- transitive helper dependencies.

Import external packages inside the worker when appropriate.

Explicitly inject source helpers through the supported source-injection mechanisms when they belong to the runnable.

For example:

```python
def clean(value):
    return value.strip().lower()


@api.funkify(extra_objs=(clean,))
def normalize(dag, value):
    return clean(value.value())
```

Do not rely on a helper merely because it is importable or happens to exist beside the funk in the authoring source.

If changing helper code should change execution identity, ensure that helper code is actually part of the runnable.

## Distinguish Python Testing From DML Testing

Use lightweight execution such as `defunkify()` when testing ordinary Python logic.

Do not treat that as sufficient validation of DML execution semantics.

Exercise a real DML call when the change concerns:

- worker isolation;
- source capture;
- helper injection;
- serialization or codecs;
- artifacts;
- executor behavior;
- cache identity;
- provenance;
- execution DAG structure.

A function that works as ordinary Python can still fail as a DML worker.

## Let DML Own Scheduling And Reuse

Write graph operations in their logical dependency order.

Do not manually deduplicate calls because two authoring calls appear equivalent.

Do not build application-level scheduling logic merely to avoid repeated execution.

DML may represent multiple authoring call nodes while reusing the same underlying execution when the runnable and normalized inputs are identical.

Design the graph around logical computation structure and let DML manage execution reuse.

## Design Cache Boundaries, Not Cache Tricks

Think about cache structure when choosing the shape of the computation.

Split expensive work when independently reusable intermediate results are likely to matter.

Do not split cheap work solely to increase the number of cached nodes.

Do not assume that changing nearby authoring code invalidates an existing execution.

When cache behavior appears surprising, inspect the actual runnable and its DML inputs rather than assuming source-file changes imply a new identity.

## Keep Dagclass Topology Explicit

Use a dagclass when a reusable computation has meaningful named topology, parameters, and multiple related stages.

Do not introduce a dagclass merely to namespace unrelated functions.

Write dependencies using direct `self.member` references so the topology remains statically visible to DML.

Avoid hiding graph dependencies behind:

- `getattr`;
- dynamic item lookup;
- reflection;
- dynamically constructed member names;
- helper abstractions that obscure which members are read.

Inside a dagclass worker, remember that `self` participates in DML execution semantics; do not reason about it as an ordinary persistent Python instance.

Treat assignment to `self.<name>` inside a worker as graph construction, not ordinary mutation of shared object state.

Prefer explicit, readable topology over clever Python indirection.

## Keep Large Data And Artifacts In Their Native DML Form

Avoid unnecessary materialization and copying of persisted datasets, files, models, or other artifacts.

When DML already has an appropriate codec or artifact representation, preserve that representation through the graph.

Pass artifact references or graph nodes between stages rather than loading data into Python and immediately writing the same data back out.

Materialize data where computation actually happens.

## Make Results Intentional

Explicitly choose the terminal result of an authored DAG.

Do not assume successful exit from a DAG authoring context implies that the intended result was committed.

Use names for nodes that are useful for inspection, debugging, or downstream reuse, not merely to mirror every temporary Python variable.

Treat the committed result as the public output of the DAG and named intermediate nodes as useful exposed structure.

## Preserve Failure Information

Treat failures as inspectable computation state rather than something to immediately erase and recompute.

When a named call fails, inspect its execution context before restructuring the graph or retrying blindly.

Preserve enough graph structure that the failed runnable, its inputs, and relevant intermediate state remain diagnosable.

## Prefer Provenance Over Convenience

When two implementations are otherwise equivalent, prefer the one that leaves DML with a more accurate description of:

- where data came from;
- what computation produced it;
- which inputs affected it;
- which runnable executed;
- which intermediate results can be reused.

Avoid Python conveniences that turn explicit DML relationships into opaque concrete values.

## Do Not Over-DML Ordinary Python

DML should describe meaningful computation and persisted dependencies.

Do not force ordinary local control flow, formatting, tiny transformations, or implementation helpers into graph nodes unless doing so provides useful DML semantics.

A useful test is:

> Would independently identifying, caching, persisting, inspecting, or executing this operation be valuable?

If not, ordinary Python is usually the better abstraction.

## Verify Semantics After Meaningful Changes

For nontrivial authoring changes, verify the DML behavior that matters rather than only checking the final Python value.

Depending on the change, inspect:

- the committed DAG result;
- important named nodes;
- execution DAGs;
- immediate call context;
- actual funk inputs;
- the rendered runnable/source;
- whether repeated calls reuse execution as expected;
- whether changed behavior correctly changes execution identity.

Use `.context(root=False)` on representative call results when execution identity, provenance, or reuse matters.

Reload committed DAGs when testing persisted behavior rather than relying only on the still-open authoring object.

## Review For Common DML Mistakes

Before finishing authored DML code, check for:

- `.value()` calls between DML operations;
- materialized values being unnecessarily restaged;
- hidden module globals or closures in script funks;
- helpers omitted from worker source;
- arguments unrelated to a funk's behavior;
- unnecessary funk boundaries;
- expensive work grouped so broadly that useful cache reuse is lost;
- dynamic dagclass dependency lookup;
- hidden behavior-affecting configuration;
- assumptions that ordinary Python tests validate worker execution;
- assumptions that authoring-source changes automatically invalidate cache identity;
- graph structure replaced with ordinary Python plumbing.

Prefer fixing the underlying graph semantics over adding procedural workarounds.
