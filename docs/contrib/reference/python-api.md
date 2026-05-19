# Python API

## Authoring helpers

`daggerml.contrib.api` exposes the main contrib authoring surface.

| Surface | What it returns | Notes |
| --- | --- | --- |
| `api.funkify(...)` | `DelayedRunnable` | Supports decorator and wrapper forms. Default target is `uri="script"`, `adapter="local"`. |
| `api.ref(name)` | `DelayedRef` | Refers to another node in the same DAG namespace. |
| `api.load(dagname, nodename=None)` | `DelayedLoad` | Loads from another committed DAG during staging. |
| `@api.dagclass(...)` | compiled class | Compiles fields and methods into a DAG recipe at instance init time. |
| `api.run(instance, ...)` | `None` | Materializes the compiled class into a DAG, calls the entrypoint, and commits the result. |

## `funkify`

`funkify` accepts either:

- a callable,
- a `Runnable`,
- a `DelayedRunnable`,
- or no positional input yet, in decorator-builder form.

Important behavior:

- callable input is stored as `kwargs["fn"]` on the delayed runnable,
- wrapper input preserves the nested `sub` runnable chain,
- lowering happens later through the adapter registry,
- delayed refs and loads inside kwargs are resolved during DAG normalization.

For script-backed callables:

- the first parameter must be `dag`,
- defaulted parameters become call kwargs recorded on the runnable,
- unknown script kwargs are rejected,
- the generated script must parse as valid Python,
- the function must be globally definable in the rendered source.

## `dagclass` and `run`

`dagclass` is for class-shaped DAG definitions.

Key rules from the current implementation:

- compilation happens when the instance is created,
- plain methods are lowered through `funkify(..., uri="script", adapter="local")`,
- dependency inference watches `self.<name>` reads inside plain methods,
- member cycles and unknown member references fail before execution,
- reserved member names include `dag`, `dml`, `argv`, `call`, `put`, and `commit`.

`api.run(instance, ..., entrypoint=None, name=None)`:

- requires a compiled dagclass instance,
- resolves the entrypoint from the explicit argument or the class default,
- inserts compiled members into the DAG under their original names,
- calls the entrypoint and commits the result.

## Testing helpers

`daggerml.contrib.testing` exposes:

- `MockNode(value)`: minimal node-like wrapper with `.value()`.
- `MockNode.from_value(value)`: preserves real `Node` and `MockNode` instances.
- `defunkify(delayed)`: unwraps to the innermost script callable and runs it in an isolated temporary workdir.

`defunkify(...)` is only for delayed runnable chains whose innermost runnable is script-backed and still retains a callable in `kwargs["fn"]`.

## Prebuilt funks

`daggerml.contrib.funks` currently exports `docker_build`.

Effective call shape:

```python
docker_build(context_tarball, build_flags=(), repo=None)
```

Behavior:

- untars the build context through `S3Store`,
- runs `docker build`,
- returns an S3 tar `Uri` by default,
- tags and pushes to `repo` when `repo` is provided.

## Status API

`daggerml.contrib.status.status()` returns a JSON-safe report with:

- `schema_version`
- `summary`
- `adapters`
- `executors`
- `codecs`
- `diagnostics`

Use it when you need structured introspection of effective contrib registrations instead of ad hoc printing.
