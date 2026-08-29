## Context

See `proposal.md` for motivation and `specs/dagclass-namespace-compilation/spec.md` for required behavior.

Today, plain methods are converted to delayed script runnables whose `prepop` values contain `DelayedRef` objects. The delayed-action codec gives `DelayedRef` a general authoring meaning: resolve `dag[name]` when the containing value is staged. That meaning is correct for ordinary `api.funkify` use but incorrect after a runnable is adopted into a dagclass namespace.

The compiler already discovers method dependencies, topologically orders members, and can recursively embed a dagclass member graph. However, the instance stores and exposes raw delayed runnables, so direct access such as `Foo(...).main` lets unresolved references escape. Nested dagclass embedding handles only one path and does not establish a uniform namespace contract.

Script-executed method functions must remain self-contained according to the repository worker-isolation rule: only function source and explicitly injected values are available in a script worker.

## Goals / Non-Goals

**Goals:**

- Give every dagclass instance one explicit compilation-time namespace.
- Resolve all references in adopted member graphs before compiled members become externally visible.
- Preserve dependency ordering, cycle detection, nested dagclass composition, and script cache identity where semantics remain equivalent.
- Keep ordinary, non-dagclass `api.ref` behavior unchanged.

**Non-Goals:**

- Introduce lexical scopes or qualified reference syntax outside dagclasses.
- Allow dagclass members to intentionally capture ambient caller-DAG nodes.
- Move compilation into `api.run()` or runtime execution.
- Redesign script execution or the general delayed-action codec.

## Decisions

### Build one namespace incrementally during instantiation

Compilation will create a member map `M` initialized only with evaluated, non-compiled attributes. It will process remaining members in dependency order. Before adding a member to `M`, the compiler will recursively replace each of its `DelayedRef(name)` values with `M[name]`; absence from `M` is a compilation error. The resolved member is then added to `M`.

This makes ordering observable inside the compiler without creating runtime lookup semantics:

```text
evaluate attrs -> M
                    |
topological member  | ref(name) -> M[name]
        |           |
        +---------> add resolved member to M
```

Alternative: retain symbolic refs plus a separate namespace object interpreted by codecs. Rejected because it would extend runtime representation and codec semantics when compilation can produce the existing closed runnable graph.

### Treat every adopted delayed reference as dagclass-local

The recursive binder will traverse all supported member graph positions, including delayed and concrete runnable `sub`/`kwargs` values and supported containers. Once a value is a dagclass member, unqualified `api.ref` no longer means caller lookup; it names another dagclass member.

This includes externally defined funkify wrappers assigned as class or instance attributes. Such a wrapper compiles only if each delayed reference is available in `M` when the wrapper is processed.

Alternative: distinguish refs created by method analysis from refs already present in external funks. Rejected because two reference meanings inside one namespace would preserve ambient leakage and make composition order-dependent in a non-local way.

### Store and expose only resolved compiled members

The instance's member map and instance attributes will contain the resolved members produced during compilation. Therefore direct method extraction, nested dagclass adoption, and entrypoint lookup all observe the same closed graph. Raw symbolic forms may exist transiently during analysis but are not the compiled instance representation.

Alternative: retain separate raw and exported member maps. Rejected because `api.run()` does not need unresolved references if resolved members are staged in topological order, and dual representations risk semantic drift.

### Keep api.run as an executor of compiled state

The `dagclass`-wrapped initializer remains the compilation boundary. `api.run()` validates that the object is compiled, obtains the configured entrypoint from the resolved member map, stages the compiled namespace as required by existing execution behavior, calls the entrypoint, and commits the result. It does not repair or resolve references.

Alternative: centralize closure building in `api.run()`. Rejected because direct member reuse is a supported composition path and must have identical semantics.

### Detect invalid references before encoding into a caller DAG

Reference validity is checked while constructing `M`, not later through `DelayedActionCodec`. Errors should identify the referencing member and unavailable name where practical. Existing dependency-cycle errors remain compilation errors.

Alternative: let caller staging fail naturally. Rejected because same-named caller nodes turn invalid definitions into silently incorrect computations.

## Risks / Trade-offs

- [Existing dagclasses may rely accidentally on caller-DAG lookup] -> Treat this as correction of unsafe behavior; document constructor attributes as the explicit injection mechanism.
- [Recursively embedding shared members can enlarge nested runnable values] -> Reuse immutable values where possible and test representative transitive/nested graphs; correctness and stable cache identity take priority over symbolic compactness.
- [Traversal misses a supported container or concrete `Runnable`] -> Centralize namespace binding in one recursive routine and cover every shape accepted by dependency collection and literal normalization.
- [Topological order currently derives from refs that must be validated against a partially built namespace] -> Separate dependency discovery from binding: discover names first, sort, then resolve each member against `M` in order.
- [Changing stored member form can affect cache keys] -> Add equivalence tests showing semantically identical dagclass and explicitly closed funk graphs normalize identically where expected.

## Migration Plan

1. Add contract tests that reproduce caller-name collision and missing-caller-name cases, plus invalid external funk references.
2. Change instantiation-time compilation to build and expose the resolved namespace incrementally.
3. Update `api.run()` tests to verify it consumes compiled state without resolving ambient names.
4. Update authoring documentation to define dagclass-local `api.ref` behavior and the external-funk sharp edge.

No persisted-data migration is required. Rollback is a code revert; compiled dagclass objects are process-local authoring values rather than persisted schema objects.
