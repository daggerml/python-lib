# Authoring And Runnables

Most contrib features exist to let you describe work now and choose where it runs later.

## The core idea

Core DaggerML gives you DAGs, nodes, refs, and commits. Contrib adds a second layer for authoring runnable work:

- `api.funkify(...)` wraps a callable or sub-runnable as a `DelayedRunnable`.
- `api.ref(name)` and `api.load(dagname, nodename=None)` create delayed references that are resolved when a DAG is staged.
- `api.dagclass` lets you declare a DAG as a Python class, then compiles its members into delayed values.
- `api.run(instance, ...)` materializes that compiled class into a fresh DAG and commits the selected entrypoint result.

The important boundary is that these helpers are declarative until the DAG normalizes values into concrete `Runnable` objects.

## `funkify` is a lowering step, not an execution step

`funkify` does not run your function. It records intent:

- which adapter to use,
- which executor or target URI to aim at,
- any wrapper chain around a sub-runnable,
- any delayed values in kwargs.

When the DAG later stages the value, contrib resolves the selected adapter from the adapter registry and asks it to produce a concrete `Runnable`.

That means nested wrappers compose naturally. A function can be script-backed first, then wrapped for Docker, then wrapped again for SSH.

## Script-backed functions are serialized source

The default contrib path is `@api.funkify(uri="script", adapter="local")`.

That path serializes the function source into an S3-backed script artifact and records metadata such as:

- the function name,
- defaulted call kwargs,
- any `prepop` values,
- any extra helper objects or source lines you injected.

The worker only gets the serialized function source plus `extra_objs` and `extra_lines`. It does not get your module globals for free. If a script-backed function needs an import at runtime, import inside the function body or inject the dependency explicitly.

## `dagclass` turns a class into a DAG recipe

`@api.dagclass` compiles an instance at `__init__` time.

During compilation, contrib:

- collects fields and plain attributes,
- compiles plain methods into script-backed delayed runnables,
- infers member dependencies from `self.<name>` reads in plain methods,
- builds a dependency graph across members,
- topologically orders the members for later materialization.

`api.run(...)` then creates a DAG, inserts each compiled member by name, calls the chosen entrypoint, and commits the result.

Use `dagclass` when you want a reusable DAG definition with explicit member names and dependencies. Use plain `funkify` when a direct callable wrapper is enough.

## Delayed values stay in the same namespace

`api.ref("name")` is for local DAG references. Inside a `dagclass`, it refers to the member namespace that `api.run(...)` later materializes.

`api.load(...)` is for loading from another committed DAG. It participates in lowering, but it does not create a local member-ordering dependency the way `ref(...)` does.

## Where prebuilt helpers fit

Contrib also ships a few reusable helpers on top of the same model:

- `daggerml.contrib.funks.docker_build` is a prebuilt script-backed delayed runnable for building container images.
- `daggerml.contrib.testing.defunkify(...)` peels a delayed runnable back to the innermost script callable for author-code unit tests.
- `daggerml.contrib.testing.MockNode` gives those tests the minimal `.value()` behavior many contrib callables expect.

Next: [Runtime model](runtime.md)
