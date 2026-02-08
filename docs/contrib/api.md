---
status: specified
doc_type: spec
---

# Contrib API

## Authority

This document is authoritative for contrib tools that create `Runnable` values:

- `api.funkify` decorator/wrapper behavior,
- `api.run` dagclass execution behavior,
- `api.ref` delayed reference behavior,
- `api.load` delayed DAG-load behavior,
- `DelayedActionCodec` behavior for contrib delayed actions,
- `api.dagclass` compilation behavior,
- lowering from `api.dagclass` instance methods to `api.funkify` materialization.

## Purpose

Define contrib API contracts for authoring `Runnable` values directly (`api.funkify`) and through delayed-reference/load helpers plus `api.dagclass` compilation.

## Scope

This doc defines:

- `dagclass(...)` declaration and compilation behavior,
- `run(...)` dagclass execution behavior,
- `funkify(...)` API shape and lazy materialization behavior,
- `ref(...)` delayed-reference behavior,
- `load(...)` delayed DAG-load behavior,
- `DelayedRunnable` construction/materialization/error contracts,
- `DelayedActionCodec` type matching and encode contracts,
- class-based DAG dependency analysis and ordering,
- inferred `prepop` construction and merge rules,
- lowering constraints for helper DSL surfaces.

This document does not redefine adapter/executor payload contracts.

## Contract References

- Contrib runtime contract: [runtime-contract.md](runtime-contract.md)
- Contrib registries: [registries.md](registries.md)
- Core adapter boundary: [../adapter-execution-contract.md](../adapter-execution-contract.md)
- Core execution model: [../execution-model.md](../execution-model.md)
- Public `api.Dag` access surface: [../api.md](../api.md)
- Error contracts: [../errors.md](../errors.md)

## Content

### Delayed Reference/Load Contract

- `api.ref` creates a delayed reference to a node in the dag. It allows users to reference nodes by name with lazy evaluation.
- `api.load` creates a delayed DAG/node load action so users can refer to values from other DAG contexts without eager execution.
- `api.ref(name)` MUST return `DelayedRef`.
- `api.load(dagname, nodename=None)` MUST return `DelayedLoad`.

Contract and invariants:

- `api.ref` and `api.load` MUST be implemented as delayed-action values handled by codec-driven normalization/lowering.
- Because they lower through codecs, `api.ref` and `api.load` MUST be valid anywhere normal input values are accepted.
- `api.ref` and `api.load` MUST remain declarative at construction time and MUST NOT execute during declaration.
- Delayed actions MUST execute only when staged/normalized into a DAG (for example via DAG insertion/compilation paths).
- For `api.load(..., nodename=None)`, resolution MUST target the DAG result node (not a node named `"result"`); DAG result-node semantics are authoritative in [../api.md](../api.md), and this document is informative-only for that core API detail.
- `api.ref` and `api.load` usage in class-based DAG definitions MUST lower through the same dependency/prepopulation model defined in this document.

### DelayedActionCodec Contract

- `DelayedActionCodec` MUST be the contrib codec responsible for delayed-action lowering.
- `DelayedActionCodec.can_encode(obj)` MUST return `isinstance(obj, (DelayedRef, DelayedLoad, DelayedRunnable))`.
- `DelayedActionCodec.encode(...)` MUST perform lowering using index context.
- `IndexOps` execution/staging semantics are authoritative in [../internal/ops/index-ops.md](../internal/ops/index-ops.md); this document is informative-only for `IndexOps` internals.
- `encode(...)` MUST apply delayed-action behavior at DAG staging/normalization time, not at delayed-action construction time.
- `encode(...)` MUST preserve deterministic behavior for identical delayed-action inputs and repository state.

### Funkify Contract

`api.funkify` is the contrib entrypoint for turning callables, runnables, and wrapper inputs into delayed runnable actions.
It captures execution intent first, then lowers that intent into concrete `Runnable` values during DAG staging/materialization.

Behavior summary:

- declaration (`api.funkify(...)`) minimally normalizes `sub_or_fn` and records adapter/URI/sub/kwargs intent in `DelayedRunnable`,
- lowering/materialization resolves adapter behavior from registries and emits concrete `Runnable` values used by execution paths.

Contract and invariants:

- **API surface:**
  - `funkify(sub_or_fn=None, *, adapter="local", uri="script", **kwargs)` MUST support decorator and wrapper forms.
  - `api.funkify(...)` MUST return `DelayedRunnable`.
- **Declaration flow (`api.funkify(...)`):**
  - MUST first resolve `sub_or_fn` to the delayed sub-runnable input used for delayed state.
  - MUST construct delayed state equivalent to:
    - `DelayedRunnable(uri=uri, adapter=adapter, sub=sub, kwargs=kwargs)`.
  - declaration MUST be side-effect free except normalization/parsing needed to build delayed state.
 - **Callable retention:**
   - when `sub_or_fn` is callable, `funkify(...)` MUST retain that callable in `DelayedRunnable.kwargs["fn"]`.
   - when `sub_or_fn` is `DelayedRunnable` or `Runnable`, wrapper construction MUST NOT add or rewrite a top-level callable metadata field on `DelayedRunnable`.
   - testing/debug access to the innermost script callable is authoritative in [testing.md](testing.md) via `defunkify(...)`.
- **Lowering flow (`DelayedActionCodec.encode`):**
  - for `DelayedRunnable dr`, codec lowering MUST execute:
    - recursively lower nested `sub` inside-out,
    - resolve `adapter_spec` from the contrib adapter registry,
    - call `adapter_spec.resolve_runnable(dr.uri, dr.kwargs, dr.sub)`.
  - `resolve_runnable(...)` MUST return a concrete `Runnable`.
  - delayed values inside returned `Runnable` fields (including `sub` and `kwargs`) MUST be resolved by recursive codec traversal; no separate delayed-materialization helper path is allowed.
- **Kwargs and lazy values:**
  - adapter runnable-resolution occurs at materialization/lowering time and MAY perform side effects required for kwargs resolution.
  - lazy references to nodes in other committed DAGs are allowed and MUST resolve through codec recursion at materialization time.
- **Deterministic failures:**
  - unknown adapter,
  - invalid sub type,
  - callable `fn` kwarg collision,
  - sub-cycle detection,
  - adapter runnable-resolution failures,
  - invalid `resolve_runnable` return type/shape.

### Dagclass Contract

`api.dagclass` MUST:

- support `@api.dagclass(entrypoint="main")` declaration,
- support class-body node declarations via field assignment,
- support `dataclasses.field(...)` for dataclass-managed field declaration,
- users MAY use `dataclasses.field(default_factory=...)` for field default construction,
- `dataclasses.field(default_factory=...)` return values MAY be:
  - a dagclass instance,
  - a delayed node handle,
  - a literal value,
- when `default_factory` returns a dagclass instance, field binding defaults to that instance's configured entrypoint,
- when `default_factory` returns a delayed node handle, that handle is bound directly,
- when `default_factory` returns a literal, that literal is bound directly,
- direct class-body assignment from a dagclass instance (for example `foo = OtherDag(...)`) resolves to that instance's configured entrypoint with the same binding semantics,
- direct class-body dagclass instantiation is discouraged because it compiles at module/class load time (including tooling/import contexts such as linters),
- treat declared fields as node declarations in compilation,
- support instance methods compiled into `funkify` delayed-runnables,
- treat explicit `@api.funkify(...)` on a dagclass method as an already-declared delayed runnable that is not recompiled by dagclass compilation,
- derive method dependencies from method syntax trees (including references to `self.<name>` and calls to `self.<name>(...)`),
- produce a topologically ordered dagclass member materialization order before `api.run(...)` inserts members into the DAG namespace.

Dagclass lifecycle rules:

- compilation happens at instance `__init__` time and produces delayed actions/literals for that instance,
- no recompilation path exists after instance construction,
- `@api.dagclass` default entrypoint is `"main"` unless explicitly overridden.

### Dependency Analysis Contract

Dependency graph construction rules:

- each compiled instance method contributes one graph node keyed by method name.
- reads/calls of `self.<name>` in method syntax create dependency edges.
- edges may target fields or methods in the same class DAG definition.
- helper DSL references (`ref`, `load`) that appear in materialized member values, explicit delayed values, or explicit `prepop` configuration MUST participate in member dependency extraction and ordering; plain-method syntax analysis does not inspect method bodies for `api.ref(...)` or `api.load(...)`.
- a `self.<name>` read MUST create a dependency only when that read may observe the class DAG binding rather than a method-local reassignment.
- a `self.<name>` read MUST NOT create a dependency when every control-flow path reaching that read assigns `self.<name>` earlier in the same method.
- augmented assignment to `self.<name>` (for example `self.x += y`) MUST count as a read dependency on `name`.

Validation rules:

- dependency cycles MUST fail before execution,
- unknown dependency references MUST fail before execution,
- reserved names (`dag`, `dml`, `argv`, `call`, `put`, `commit`) MUST be rejected.
- unsupported dynamic/self-reflective constructs MUST fail before execution, including:
  - `del self.<name>`,
  - `getattr(self, ...)`,
  - `setattr(self, ...)`,
  - `hasattr(self, ...)`,
  - writes to names that resolve to compiled methods,
  - closures, lambdas, comprehensions, generators, or async methods that capture `self` for dependency-bearing access.
- item-style self access (for example `self["x"]`) is allowed as an escape hatch, but dagclass compilation MUST treat it as opaque: it MUST NOT infer dependencies from that access pattern and MUST NOT use it for inferred `prepop` construction.

### Method Lowering Contract

Method lowering happens before dagclass member ordering.

For each plain method, the compiler MUST:

1. compute inferred dependencies for that method from the dependency graph.
2. construct inferred `prepop` as `dict[str, Node]` keyed by dependency name.
3. compile plain methods by calling `funkify` with the inferred `prepop`.

Rules:

- dagclass compilation of plain methods MUST lower through `funkify` and MUST NOT use a separate runnable-materialization path.
- each compiled plain method is implicitly lowered with baseline config equivalent to:
  - `funkify(method_fn, uri="script", adapter="local", prepop=inferred_prepop, ...)`.
- explicit `@api.funkify(...)` on a dagclass method means that method is already declared as a delayed runnable and MUST NOT be recompiled or have inferred `prepop` merged into it.
- methods compiled through dagclass MUST preserve their method name as the DAG node name used by `api.run(...)` materialization.
- method compilation MUST NOT require topological ordering among methods; inferred method dependencies MUST be represented through delayed references in the compiled `DelayedRunnable` payload.

### Compilation Contract

Dagclass compilation has three phases:

1. plain-method compilation into `DelayedRunnable` values,
2. recursive dependency extraction across all dagclass members,
3. topological ordering and validation.

Compilation order and node creation rules:

- fields and class attributes resolve to bound literal/delayed values first,
- plain methods compile into named `DelayedRunnable` values before member ordering,
- dependency extraction then traverses all dagclass members, including fields, class attributes, compiled methods, explicit delayed values, and nested container payloads,
- topological ordering then produces the canonical member materialization order for `api.run(...)`,
- compilation MUST produce a stable per-instance mapping of compiled dagclass members by name for later run-time DAG materialization.

Wrapper composition rule:

- method compilation first applies implicit script-layer funkify wrapping,
- explicit `@api.funkify(...)` methods are taken as already-wrapped delayed runnables and are not rewrapped by dagclass compilation.

Rules:

- all compiled method execution payloads MUST be script-executor compatible and satisfy [runtime-contract.md](runtime-contract.md).
- compiled callables MUST execute against public `api.Dag` directly (`dag.foo` / `dag["foo"]`) without requiring wrapper adaptation.
- compilation output MUST preserve enough ordering information for `api.run(...)` to materialize fields and methods into a fresh DAG namespace deterministically.
- compilation MUST store the final ordered member name list on the compiled dagclass instance for later run-time namespace materialization.

### Member Dependency Graph Contract

After plain-method compilation, dagclass compilation MUST build one member dependency graph over all materialized dagclass members.

Dependency extraction rules:

- each dagclass member contributes one graph node keyed by member name,
- member dependency extraction MUST recurse through supported delayed/container values, including:
  - `DelayedRef`,
  - `DelayedLoad`,
  - `DelayedRunnable`,
  - realized `Runnable` values,
  - lists, tuples, dicts, and nested combinations of those values,
- `ref("name")` and equivalent delayed references to another dagclass member MUST create a dependency edge to that member,
- external DAG loads via `load(...)` MUST NOT create local dagclass-member ordering edges,
- plain-method dependencies encoded in compiled `DelayedRunnable` payloads MUST participate in the same member dependency graph as field/class-attribute dependencies,
- dependency extraction MUST NOT inspect arbitrary plain method bodies beyond the syntax-based method compilation rules in this document.

Validation rules:

- unknown local member references discovered during member dependency extraction MUST fail before execution,
- dependency cycles across any dagclass members MUST fail before execution,
- self-referential local member cycles MUST fail before execution,
- the resulting topological order MUST include every materialized dagclass member exactly once.

### Script Prepopulation Contract

Compiled `prepop` is consumed by the script executor at run time.

Rules:

- compiled `prepop` keys MUST be strings and MUST satisfy class DAG name validation.
- compiled `prepop` values MUST resolve to serializable values under `funkify` materialization rules.
- inferred `prepop` is created only from plain-method dependency analysis of `self.<name>` access.
- helper DSL values such as `ref(...)` and `load(...)` MAY also appear in explicitly declared delayed values, field values, class attributes, and explicit `@api.funkify(...)` method configuration; those values participate in member dependency extraction and ordering even when they are not inferred from plain method bodies.
- script runtime prepopulation behavior and executor-specific kwargs/schema details are authoritative in [executor-catalog.md](executor-catalog.md); this document is informative-only for those executor-internal details.

### Dagclass Run Contract

- `api.run(instance, *args, name=None, entrypoint=None, **kwargs)` is the dagclass execution entrypoint.
- `api.run(...)` returns `None`.
- `instance` MUST be a compiled dagclass instance.
- run entrypoint resolution order is:
  - explicit `entrypoint` argument,
  - dagclass default entrypoint from `@api.dagclass(...)`.
- resolved entrypoint MUST be `DelayedRunnable`.
- `api.run(...)` MUST NOT perform compilation or recompilation.
- `api.run(...)` MUST:
  - create a DAG using resolved run name,
  - materialize all compiled dagclass fields and methods into that DAG as named nodes before entrypoint invocation,
  - preserve compiled dagclass member names when inserting those nodes so class-local references (including `api.ref(...)`) target the same DAG namespace,
  - invoke the selected entrypoint from that materialized DAG namespace with forwarded `*args`/`**kwargs`,
  - commit the resulting value.

Run naming rules:

- default run name is `{path-to-dag-file-relative-to-repo-root-without-extension}::{dag-class-name}`.
- if a repository root is discoverable, default naming uses path relative to that repository root.
- if no repository root is discoverable, default naming uses path relative to current working directory.
- explicit `name` overrides default run name.

Deterministic run failures include:

- instance is not a dagclass instance,
- resolved entrypoint is missing,
- resolved entrypoint is not `DelayedRunnable`,
- compiled dagclass namespace is incomplete or internally inconsistent,
- invalid run name,
- argument binding/invocation contract violations.

Example:

```python
from dataclasses import field

from daggerml.contrib import api


@api.dagclass(entrypoint="bar")
class MyDag:
    foo = field(default_factory=lambda: OtherDag(param0=23, param1=577))
    n = field(default_factory=lambda: 2)

    @api.funkify(uri="batch", adapter="lambda", lambda_uri=api.ref("batch"))
    def bar(self, x, y=23, *, z=5):
        return self.foo(x, y, z)


res = api.run(MyDag(), 1, z=9)
```

Execution staging internals are authoritative in [../internal/ops/index-ops.md](../internal/ops/index-ops.md); this document is informative-only for internal staging mechanics.

### Helper DSL Boundary

Contrib DAG helper surfaces (for example `ref`, `load`) are allowed, but they MUST preserve this lowering model:

- helper usage MUST lower to dependency edges and/or explicit `prepop`,
- helper usage MUST NOT introduce a serializer path separate from `funkify`,
- helper usage MUST preserve deterministic topological compilation.

## References

- [runtime-contract.md](runtime-contract.md)
- [registries.md](registries.md)
- [../adapter-execution-contract.md](../adapter-execution-contract.md)
- [../execution-model.md](../execution-model.md)
- [../errors.md](../errors.md)
