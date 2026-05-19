# Type System

The type system in `src/daggerml/_internal/types.py` is the contract that keeps the repository coherent. It is less about static typing in Python and more about making sure persisted objects have a predictable shape and reference each other correctly.

## Namespace registry first

Everything starts with `NAMESPACES`, a runtime registry that maps namespace strings to Python classes.

Two families register themselves automatically:

- `Datum` subclasses become `datum-*` namespaces,
- `Node` subclasses become `node-*` namespaces.

Other persisted objects such as `Dag`, `Tree`, `Commit`, `Error`, and `Deletable` register through `_register_dml_obj`.

This is what lets the storage layer turn `Ref("commit:...")` or `Ref("datum-list:...")` back into the right dataclass on read.

## Core object families

### Datums

Datums are the stored value layer.

- `ScalarDatum` stores Python scalars.
- `ListDatum` and `DictDatum` store refs to other datums, not embedded Python objects.
- `Uri` stores external locations.
- `RunnableDatum` stores executable specifications in a repository-friendly form.

One subtle but important split is `Runnable` vs `RunnableDatum`.

- `Runnable` is the public, fully materialized Python object.
- `RunnableDatum` is the internal persisted form, where `target`, `sub`, and `kwargs` are refs to other stored objects.

That split keeps the repository graph explicit even when the Python API exposes something friendlier.

### Nodes

Nodes are the computation layer.

- `LiteralNode` points directly at a datum or error-like value.
- `ArgvNode` and `KwargvNode` are specialized literals used to anchor function inputs.
- `ImportNode` imports a value from another DAG.
- `FnNode` points at a child DAG created by a function call.

Each node knows how to turn itself into a datum ref through `datum_ref(txn)`, which is the common interface used by `NodeOps` and the execution path.

### Graph and history objects

- `Dag` gathers nodes, names, and either a result or an error.
- `Tree` is a named map of DAG refs.
- `Commit` is a versioned snapshot pointing at a tree and optional focal DAG.

Together they form the repository's history model: commits snapshot trees, trees name DAGs, DAGs connect nodes, and nodes reach stored values.

### Errors as data

`Error` and `DmlRepoError` are not only in-memory exceptions. They are part of the persisted model too. A failed computation can be captured as structured error state with message, origin, type, and stack frames, then referenced from a DAG.

That makes failures inspectable through the same object graph as successful results.

## Validation strategy

Every persisted object validates itself in `__post_init__()` by calling `_validate()`.

In practice, validation checks three things:

- field shapes are correct,
- refs point to the expected namespace family,
- impossible combinations, such as a DAG with both `result` and `error`, are rejected early.

The helper `require_ref()` is used throughout the model to check both "is this a ref?" and "does its namespace hierarchy match what this field expects?"

## Why contributors feel this layer everywhere

The type system is not a side file. It affects almost every subsystem:

- `BaseOps` depends on it for serialization and deserialization,
- the ops layer depends on it for field and namespace guarantees,
- remote manifests depend on stable namespace and object-id behavior,
- the public API depends on it when staging Python values into stored datums.

If you are changing object shape, namespace rules, or how values cross the public/internal boundary, this is usually the first file to inspect.
