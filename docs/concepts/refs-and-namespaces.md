# Refs and namespaces

Refs are the connective tissue of DaggerML. Nearly every persisted relationship is expressed as a typed ref rather than an embedded object.

## Ref shape

A ref is a string-like identity in the form `namespace:id`.

The namespace tells you what kind of object the ref is expected to resolve to. The id is the stable identity inside that namespace.

Examples from the current model include:

- `dag`, `commit`, `tree`, `head`, `index`
- `node-literal`, `node-fn`, `node-import`, `node-argv`, `node-kwargv`
- `datum-scalar`, `datum-list`, `datum-dict`, `datum-uri`, `datum-runnable`
- `error`, `deletable`

## Why namespaces matter

Namespaces are not just prefixes for display. They are part of validation and runtime safety.

The internal type layer checks namespace expectations whenever an object says it should point at a DAG, node, datum, commit, or other stored type. Using the right namespace is how DaggerML keeps object graphs explicit and well-typed even though everything is connected through refs.

## Refs are how objects stay shareable

Because DAGs, commits, nodes, and data all point at each other through refs:

- objects can be content-addressed and deduplicated
- multiple higher-level objects can share the same lower-level object
- remote sync can transfer object graphs without depending on Python object identity

That is especially visible in two places:

- DAGs refer to nodes and result/error refs
- commits refer to trees, and trees refer to DAGs

## Names are different from refs

User-facing names such as DAG names, branch names, or `Dag.names` entries are lookup handles. They are mutable labels that eventually resolve to refs.

Refs are the durable identities underneath those labels.

That distinction helps when reading the system:

- names help humans navigate
- refs are what the storage model actually links together

## How to think about it

If you are ever unsure what an object relationship means, look for the ref and its namespace. In DaggerML, that usually tells you both the target object family and the layer boundary being crossed.

See also:

- [Storage](storage.md)
- [Commits and history](commits-and-history.md)
- [Remotes](remotes.md)
