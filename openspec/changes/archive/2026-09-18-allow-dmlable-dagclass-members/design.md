## Context

Dagclass instantiation currently collects dataclass fields separately from class-defined members. Fields are accepted as evaluated values, while class-defined members pass through explicit descriptor and generic-callable rejection after recognized method forms are detected. This makes classification depend on declaration style and prevents callable codec inputs such as `Node` and `Projection` from reaching `Dag`-owned normalization.

Compilation must continue to resolve dagclass-local delayed references and nested dagclass entrypoints before execution. Codec availability and contextual validity, however, depend on the target DAG and cannot be established reliably during instance construction.

## Goals / Non-Goals

**Goals:**

- Give dataclass fields and class-defined ordinary values the same acceptance boundary.
- Preserve existing recognition and compilation of plain methods and decorated self-methods.
- Preserve nested dagclass replacement and dagclass-local delayed-reference binding.
- Let the existing codec path determine whether an ordinary value can be staged.

**Non-Goals:**

- Do not broaden which Python functions are treated as dagclass methods.
- Do not add a compile-time codec probe or a new definition of "DML-able."
- Do not relax codec rules for cross-index nodes, projections, or custom values.
- Do not change dependency inference from direct `self.<name>` syntax.

## Decisions

### Classify only forms that dagclass compilation transforms

Class member collection will retain an ordered set of positive recognizers:

1. A plain Python function is a method definition to compile.
2. A delayed runnable containing a decorated function whose first parameter is `self` is a decorated method to analyze and augment.
3. A nested dagclass instance is replaced by its configured compiled entrypoint.
4. Every other evaluated member is an ordinary namespace value.

The generic callable and descriptor rejection categories will be removed. This is preferable to adding exceptions for `Node` and `Projection` because codec plugins can support other callable types, and future value wrappers should not require changes to dagclass classification.

An alternative was to ask the codec registry whether each value is supported during compilation. That was rejected because codec encoding requires an active target `Dag`, can mutate that DAG, and may enforce contextual rules that are unknowable at instance construction.

### Preserve evaluated member values rather than raw class dictionary entries

Ordinary class-defined members will continue through normal instance attribute lookup before entering the namespace. This keeps them aligned with the existing requirement that compilation starts from evaluated instance attributes and retains Python descriptor behavior. A descriptor that raises while being evaluated may still fail instantiation; the removed behavior is the categorical rejection based only on descriptor type.

An alternative was to preserve raw descriptor objects from the class dictionary. That would change normal Python attribute semantics and expose descriptor implementation objects to codecs instead of the value visible through the instance.

### Defer value support errors to DAG staging

Compilation will not call `apply_codecs()` or otherwise validate ordinary values. When a compiled delayed graph is staged, recursive normalization will process captured members exactly as it processes equivalent direct or nested DAG inputs. Unsupported values and contextually invalid nodes will therefore raise existing codec errors at staging time.

This keeps ownership consistent with the codec-normalization capability and avoids a second, incomplete staging model inside dagclass compilation.

### Keep namespace-aware recursive binding narrow

The compiler will continue recursively collecting and binding `DelayedRef` values only through the delayed runnable, concrete runnable, and built-in collection structures it already understands. Arbitrary codec-backed objects remain opaque to dagclass dependency analysis. Custom values that need dagclass-local references must expose those references through an existing recognized wrapper rather than hiding them inside codec-specific state.

General traversal through arbitrary custom objects was rejected because the compiler has no protocol for discovering or rebuilding their internal references.

## Risks / Trade-offs

- [Unsupported callable values fail later during staging instead of during instantiation] -> Document that dagclass compilation validates transformed forms and namespace references, while codecs validate ordinary values.
- [Descriptor lookup can execute user code during compilation] -> Retain ordinary Python instance lookup semantics and test that only categorical descriptor rejection is removed; do not promise side-effect-free descriptor evaluation.
- [A mistakenly assigned callable object is no longer diagnosed as an unsupported method] -> The value will either be accepted by its codec or fail with the standard codec error, providing one consistent support boundary.
- [Custom codec objects containing hidden `DelayedRef` values are not namespace-bound] -> Keep the existing explicit traversal boundary and document wrappers as the supported way to express dagclass-local references.

## Migration Plan

No data migration or compatibility shim is required. The change removes eager rejection, so existing valid dagclasses retain their behavior while newly accepted values proceed to normal staging. Rollback consists of restoring the compile-time callable and descriptor checks; no persisted representation changes are introduced.
