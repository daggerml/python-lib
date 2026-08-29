## Context

See `proposal.md` for motivation. A script-executed dagclass method receives a `daggerml.api.Dag` as its first argument, even though its source names that argument `self`. Python therefore resolves instance fields, class attributes, methods, and properties before `Dag.__getattr__` can interpret an attribute as a named-node lookup.

The current analyzer walks statements while carrying a definite-assignment set. It requires assignment targets to be declared members, rejects useful worker-DAG node creation, and treats `self.put` as an unknown member instead of a `Dag` operation. The compiler only needs a dependency topology; executing or approximating arbitrary Python control flow would make compilation complex and surprising.

## Goals / Non-Goals

**Goals:**

- Make inferred topology match Python attribute lookup when `self` is a worker `Dag`.
- Compute edges with deterministic, control-flow-independent syntax analysis.
- Allow methods to create named nodes through direct attribute assignment.
- Reject every inferred edge whose source or destination is outside the dagclass collection.
- Give users prominent documentation for the deliberately sharp analysis boundaries.

**Non-Goals:**

- Prove assignment ordering, reachability, or definite assignment.
- Infer dependencies or assignments from item access, aliases, `getattr`, `setattr`, or other dynamic Python behavior.
- Make every reserved `Dag` operation useful or advisable inside dagclass workers.
- Change script serialization, runtime invocation, DAG storage, or cache identity.

## Decisions

### Collect references and assignments independently

Analyze each method body into two name sets:

```text
references  = names directly loaded as self.<name>
assignments = names directly targeted as self.<name>
edges       = references - assignments - reserved_names
```

Deduplicate names while preserving stable source order where dependency ordering or generated `prepop` dictionaries expose it. Validate the resulting edges only after subtraction. This allows an undeclared assigned name while preserving compilation errors for undeclared, unassigned references.

This replaces the current statement-by-statement defined-name propagation. A control-flow-aware alternative was rejected because it cannot fully model Python without effectively evaluating code and conflicts with the explicit assumption that users create assigned nodes before loading them.

### Define assignments by direct attribute assignment targets

Treat direct AST assignment targets for `self.<name>` as assignment evidence throughout the method body. Ordinary, annotated, chained, destructured, loop-target, and equivalent store contexts should follow the same rule when represented as direct attribute stores. An augmented assignment may follow the same syntactic exclusion rule even though arithmetic directly between ordinary values and DaggerML nodes is generally not useful.

Do not require an assigned non-reserved name to exist in the class member collection. At runtime, `Dag.__setattr__` routes such names to named-node insertion. Assignment to a declared method or attribute also suppresses that method's edge because the worker method is explicitly creating an invocation-local named node with that name.

### Derive reserved names from worker Dag lookup behavior

Build the reserved-name set from public `Dag` dataclass fields plus public attributes available on the `Dag` type. This captures the current fields, properties, and operations without maintaining a second hand-written API list, and automatically follows additions to the worker `Dag` surface. Names beginning with `_` remain outside named-node behavior because `Dag.__getattr__` rejects them.

The current public collision surface includes:

```text
dml, token, ref, name, message, tags,
argv, result, keys, values,
put, require, call, commit, freeze, unfreeze, cancel
```

`dag` is not reserved. Reserved means only that normal Python lookup wins over named-node attribute access; it does not advertise or guarantee that invoking the attribute is useful in a dagclass worker.

Continue rejecting class members with reserved names. Reject direct assignment to reserved names because `Dag.__setattr__` treats them as ordinary instance attributes rather than named-node creation, which would violate the assignment inference contract.

A static hand-written set was rejected because it can silently drift whenever `Dag` gains or loses a public field, property, or method.

### Keep item access opaque

Do not inspect `self[...]` operations, even when the key is a string literal. Item loads add no edges, and item assignments remove no edges. Supporting literal keys now would create a partial second syntax with unresolved questions around dynamic keys and aliases.

Documentation must label this prominently as a sharp bit: item access still executes according to `Dag.__getitem__` and `Dag.__setitem__`, but compilation provides no topology or dependency binding for it.

### Validate only final edges

After reference, assignment, and reserved-name processing, require both the referencing method and every destination to exist in the dagclass member collection. Unknown remaining destinations produce a compilation error. Names removed by assignment are local worker-DAG names and are not edge endpoints, so they are not subject to member validation.

## Risks / Trade-offs

- [A conditional or later assignment suppresses a dependency even when runtime reads first] -> Document the assign-before-read assumption prominently and retain the normal missing-node runtime error.
- [Item access can bypass topology inference and fail at runtime] -> Include side-by-side attribute and item-access examples in the authoring guide and call item access unsupported by compilation.
- [The reserved set grows when the public `Dag` surface grows] -> Treat this as intentional alignment with runtime Python lookup and cover representative fields, properties, and methods with tests.
- [Dynamic syntax such as aliases, `getattr`, and `setattr` remains invisible] -> State that compilation recognizes only direct `self.<name>` syntax.
- [Previously rejected assignment patterns become valid but can still be logically incorrect] -> Keep compiler guarantees limited to syntax and edge validity; do not imply runtime ordering validation.

## Migration Plan

Implement the analyzer and tests together, then update the capability spec and human-facing authoring guide in the same release. No persisted data migration is required. Rollback consists of reverting the compiler, tests, and documentation because compiled dagclass values are regenerated from source and no storage format changes.
