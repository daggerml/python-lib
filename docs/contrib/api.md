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

## Purpose

Define contrib API contracts for authoring `Runnable` values directly (`api.funkify`) and through delayed-reference/load helpers plus `api.dagclass` compilation.

## Glossary

- DAG: Directed Acyclic Graph.
- Runnable: A unit of execution in the daggerml system.
- DelayedRunnable: A delayed intent to execute a sub-runnable.
- DelayedRef: A lazy reference to a node.
- DelayedLoad: A lazy reference to a DAG or node within another DAG.
- Prepop: Named values injected into script-backed callable execution through script runtime metadata.

## Contract

### Interfaces

#### `api.ref`

- **Location**: `daggerml.contrib.api.ref`
- **Signature**: `api.ref(name: str) -> DelayedRef`
- **Accepted inputs and output shape**: accepts a node name string and returns `DelayedRef`.
- **Behavior/semantics**:
  - creates a delayed reference to a node in the DAG,
  - MUST be implemented as a delayed-action value handled by codec-driven normalization/lowering,
  - MUST remain declarative at construction time and MUST NOT execute during declaration,
  - MUST be valid anywhere normal input values are accepted because it lowers through codecs.
- **Side effects**: none at declaration time.
- **Invocation surfaces**: Python code, dagclass definitions.
- **Unspecified fields**: rejected.

#### `api.load`

- **Location**: `daggerml.contrib.api.load`
- **Signature**: `api.load(dagname: str, nodename: str | None = None) -> DelayedLoad`
- **Accepted inputs and output shape**: accepts a DAG name and optional node name and returns `DelayedLoad`.
- **Behavior/semantics**:
  - creates a delayed DAG/node load action so users can refer to values from other DAG contexts without eager execution,
  - MUST be implemented as a delayed-action value handled by codec-driven normalization/lowering,
  - MUST remain declarative at construction time and MUST NOT execute during declaration,
  - MUST be valid anywhere normal input values are accepted because it lowers through codecs.
- **Side effects**: none at declaration time.
- **Invocation surfaces**: Python code, dagclass definitions.
- **Unspecified fields**: rejected.

#### `DelayedActionCodec`

- **Location**: contrib codec catalog.
- **Interfaces**:
  - `can_encode(obj) -> bool`
  - `encode(obj, ctx) -> Any`
- **Accepted inputs and output shape**:
  - `can_encode(obj)` MUST return `isinstance(obj, (DelayedRef, DelayedLoad, DelayedRunnable))`.
  - `encode(...)` MUST lower delayed actions using index context.
- **Behavior/semantics**:
  - `DelayedActionCodec` MUST be the contrib codec responsible for delayed-action lowering,
  - `encode(...)` MUST apply delayed-action behavior at DAG staging/normalization time, not at delayed-action construction time,
  - `encode(...)` MUST preserve deterministic behavior for identical delayed-action inputs and repository state.
- **Invocation surfaces**: codec traversal during staging/materialization.
- **Unspecified fields**: rejected.

#### `api.funkify`

- **Location**: `daggerml.contrib.api.funkify`
- **Signature**: `funkify(sub_or_fn=None, *, adapter="local", uri="script", **kwargs) -> DelayedRunnable`
- **Accepted inputs and output shape**:
  - MUST support decorator and wrapper forms,
  - accepts a callable, `DelayedRunnable`, `Runnable`, or omitted `sub_or_fn`,
  - MUST return `DelayedRunnable`.
- **Behavior/semantics**:
  - captures execution intent first, then lowers that intent into concrete `Runnable` values during DAG staging/materialization,
  - declaration (`api.funkify(...)`) minimally normalizes `sub_or_fn` and records adapter/URI/sub/kwargs intent in delayed state,
  - declaration MUST be side-effect free except normalization/parsing needed to build delayed state,
  - declaration MUST first resolve `sub_or_fn` to the delayed sub-runnable input used for delayed state,
  - declaration MUST construct delayed state equivalent to `DelayedRunnable(uri=uri, adapter=adapter, sub=sub, kwargs=kwargs)`.
- **Callable retention**:
  - when `sub_or_fn` is callable, `funkify(...)` MUST retain that callable in `DelayedRunnable.kwargs["fn"]`,
  - when `sub_or_fn` is `DelayedRunnable` or `Runnable`, wrapper construction MUST NOT add or rewrite a top-level callable metadata field on `DelayedRunnable`.
- **Lowering flow**:
  - for `DelayedRunnable dr`, codec lowering MUST recursively lower nested `sub` inside-out,
  - lowering MUST resolve `adapter_spec` from the contrib adapter registry,
  - lowering MUST call `adapter_spec.resolve_runnable(dr.uri, dr.kwargs, dr.sub)`,
  - `resolve_runnable(...)` MUST return a concrete `Runnable`,
  - delayed values inside returned `Runnable` fields, including `sub` and `kwargs`, MUST be resolved by recursive codec traversal,
  - no separate delayed-materialization helper path is allowed.
- **Kwargs and lazy values**:
  - adapter runnable-resolution occurs at materialization/lowering time and MAY perform side effects required for kwargs resolution,
  - lazy references to nodes in other committed DAGs are allowed and MUST resolve through codec recursion at materialization time.
- **Invocation surfaces**: decorator, wrapper function, dagclass method compilation.
- **Unspecified fields**: rejected unless accepted by selected adapter/executor contracts during lowering.

#### `api.dagclass`

- **Location**: `daggerml.contrib.api.dagclass`
- **Signature**: `@api.dagclass(entrypoint="main")`
- **Accepted inputs and output shape**:
  - MUST support `@api.dagclass(entrypoint="main")` declaration,
  - users MAY declare class-body node declarations via field assignment,
  - users MAY declare dataclass-managed fields via `dataclasses.field(...)`.
- **Behavior/semantics**:
  - treats declared fields as node declarations in compilation,
  - supports instance methods compiled into `funkify` delayed-runnables,
  - treats explicit `@api.funkify(...)` on a dagclass method as an already-declared delayed runnable that is not recompiled by dagclass compilation,
  - derives method dependencies from method syntax trees, including references to `self.<name>` and calls to `self.<name>(...)`,
  - produces a topologically ordered dagclass member materialization order before `api.run(...)` inserts members into the DAG namespace,
  - compilation happens at instance `__init__` time and produces delayed actions/literals for that instance,
  - no recompilation path exists after instance construction,
  - default entrypoint is `"main"` unless explicitly overridden.
- **Field binding rules**:
  - users MAY use `dataclasses.field(default_factory=...)` for field default construction,
  - `dataclasses.field(default_factory=...)` return values MAY be a dagclass instance, a delayed node handle, or a literal value,
  - when `default_factory` returns a dagclass instance, field binding defaults to that instance's configured entrypoint,
  - when `default_factory` returns a delayed node handle, that handle is bound directly,
  - when `default_factory` returns a literal, that literal is bound directly,
  - direct class-body assignment from a dagclass instance resolves to that instance's configured entrypoint with the same binding semantics,
  - direct class-body dagclass instantiation is discouraged because it compiles at module/class load time, including tooling/import contexts such as linters.
- **Invocation surfaces**: class definition and instance construction.
- **Unspecified fields**: rejected.

#### `api.run`

- **Location**: `daggerml.contrib.api.run`
- **Signature**: `api.run(instance, *args, name=None, entrypoint=None, **kwargs) -> None`
- **Accepted inputs and output shape**:
  - `instance` MUST be a compiled dagclass instance,
  - returns `None`.
- **Behavior/semantics**:
  - is the dagclass execution entrypoint,
  - MUST NOT perform compilation or recompilation,
  - run entrypoint resolution order is explicit `entrypoint` argument, then dagclass default entrypoint,
  - resolved entrypoint MUST be `DelayedRunnable`,
  - MUST create a DAG using the resolved run name,
  - MUST materialize all compiled dagclass fields and methods into that DAG as named nodes before entrypoint invocation,
  - MUST preserve compiled dagclass member names when inserting those nodes so class-local references, including `api.ref(...)`, target the same DAG namespace,
  - MUST invoke the selected entrypoint from that materialized DAG namespace with forwarded `*args` and `**kwargs`,
  - MUST commit the resulting value.
- **Run naming**:
  - default run name is `{path-to-dag-file-relative-to-repo-root-without-extension}::{dag-class-name}`,
  - if a repository root is discoverable, default naming uses path relative to that repository root,
  - if no repository root is discoverable, default naming uses path relative to current working directory,
  - explicit `name` overrides the default run name.
- **Side effects**: creates and mutates DAG state, invokes user code, commits results.
- **Invocation surfaces**: Python API.
- **Unspecified fields**: rejected.

### Invariants

#### Delayed reference and load invariants

- `api.ref(name)` MUST return `DelayedRef`.
- `api.load(dagname, nodename=None)` MUST return `DelayedLoad`.
- `api.ref` and `api.load` MUST execute only when staged/normalized into a DAG.
- For `api.load(..., nodename=None)`, resolution MUST target the DAG result node and not a node literally named `"result"`.
- `api.ref` and `api.load` usage in class-based DAG definitions MUST lower through the same dependency/prepopulation model defined in this document.

#### Dagclass dependency analysis invariants

- Each compiled instance method contributes one graph node keyed by method name.
- Reads or calls of `self.<name>` in method syntax create dependency edges.
- Dependency edges MAY target fields or methods in the same class DAG definition.
- Helper DSL references (`ref`, `load`) that appear in materialized member values, explicit delayed values, or explicit `prepop` configuration MUST participate in member dependency extraction and ordering.
- Plain-method syntax analysis MUST NOT inspect method bodies for `api.ref(...)` or `api.load(...)` beyond the dependency rules stated here.
- A `self.<name>` read MUST create a dependency only when that read may observe the class DAG binding rather than a method-local reassignment.
- A `self.<name>` read MUST NOT create a dependency when every control-flow path reaching that read assigns `self.<name>` earlier in the same method.
- Augmented assignment to `self.<name>` such as `self.x += y` MUST count as a read dependency on that name.
- Dependency cycles MUST fail before execution.
- Unknown dependency references MUST fail before execution.
- Reserved names `dag`, `dml`, `argv`, `call`, `put`, and `commit` MUST be rejected in dagclass dependencies.
- Unsupported dynamic or self-reflective constructs MUST fail before execution, including:
  - `del self.<name>`,
  - `getattr(self, ...)`,
  - `setattr(self, ...)`,
  - `hasattr(self, ...)`,
  - writes to names that resolve to compiled methods,
  - closures, lambdas, comprehensions, generators, or async methods that capture `self` for dependency-bearing access.
- Item-style self access such as `self["x"]` is allowed as an escape hatch, but dagclass compilation MUST treat it as opaque and MUST NOT infer dependencies or inferred `prepop` from that access pattern.

#### Method lowering and compilation invariants

- Method lowering happens before dagclass member ordering.
- For each plain method, the compiler MUST:
  - compute inferred dependencies for that method from the dependency graph,
  - construct inferred `prepop` as `dict[str, Node]` keyed by dependency name,
  - compile plain methods by calling `funkify` with the inferred `prepop`.
- Dagclass compilation of plain methods MUST lower through `funkify` and MUST NOT use a separate runnable-materialization path.
- Each compiled plain method is implicitly lowered with baseline config equivalent to `funkify(method_fn, uri="script", adapter="local", prepop=inferred_prepop, ...)`.
- Explicit `@api.funkify(...)` on a dagclass method means that method is already declared as a delayed runnable and MUST NOT be recompiled or have inferred `prepop` merged into it.
- Methods compiled through dagclass MUST preserve their method name as the DAG node name used by `api.run(...)` materialization.
- Method compilation MUST NOT require topological ordering among methods; inferred method dependencies MUST be represented through delayed references in the compiled `DelayedRunnable` payload.
- Dagclass compilation has three phases:
  - plain-method compilation into `DelayedRunnable` values,
  - recursive dependency extraction across all dagclass members,
  - topological ordering and validation.
- Fields and class attributes resolve to bound literal/delayed values first.
- Plain methods compile into named `DelayedRunnable` values before member ordering.
- Dependency extraction then traverses all dagclass members, including fields, class attributes, compiled methods, explicit delayed values, and nested container payloads.
- Topological ordering then produces the canonical member materialization order for `api.run(...)`.
- Compilation MUST produce a stable per-instance mapping of compiled dagclass members by name for later runtime DAG materialization.
- Method compilation first applies implicit script-layer funkify wrapping.
- Explicit `@api.funkify(...)` methods are taken as already-wrapped delayed runnables and are not rewrapped by dagclass compilation.
- All compiled method execution payloads MUST be script-executor compatible and satisfy `runtime-contract.md`.
- Compiled callables MUST execute against public `api.Dag` directly via `dag.foo` or `dag["foo"]` without requiring wrapper adaptation.
- Compilation output MUST preserve enough ordering information for `api.run(...)` to materialize fields and methods into a fresh DAG namespace deterministically.
- Compilation MUST store the final ordered member name list on the compiled dagclass instance for later runtime namespace materialization.

#### Member dependency graph and helper DSL invariants

- After plain-method compilation, dagclass compilation MUST build one member dependency graph over all materialized dagclass members.
- Each dagclass member contributes one graph node keyed by member name.
- Member dependency extraction MUST recurse through supported delayed/container values, including:
  - `DelayedRef`,
  - `DelayedLoad`,
  - `DelayedRunnable`,
  - realized `Runnable` values,
  - lists, tuples, dicts, and nested combinations of those values.
- `ref("name")` and equivalent delayed references to another dagclass member MUST create a dependency edge to that member.
- External DAG loads via `load(...)` MUST NOT create local dagclass-member ordering edges.
- Plain-method dependencies encoded in compiled `DelayedRunnable` payloads MUST participate in the same member dependency graph as field and class-attribute dependencies.
- Dependency extraction MUST NOT inspect arbitrary plain method bodies beyond the syntax-based method compilation rules in this document.
- Unknown local member references discovered during member dependency extraction MUST fail before execution.
- Dependency cycles across any dagclass members MUST fail before execution.
- Self-referential local member cycles MUST fail before execution.
- The resulting topological order MUST include every materialized dagclass member exactly once.
- Helper usage MUST lower to dependency edges and/or explicit `prepop`.
- Helper usage MUST NOT introduce a serializer path separate from `funkify`.
- Helper usage MUST preserve deterministic topological compilation.

#### Script prepopulation invariants

- Compiled `prepop` is consumed by the script executor at runtime.
- Compiled `prepop` keys MUST be strings and MUST satisfy class DAG name validation.
- Compiled `prepop` values MUST resolve to serializable values under `funkify` materialization rules.
- Inferred `prepop` is created only from plain-method dependency analysis of `self.<name>` access.
- Helper DSL values such as `ref(...)` and `load(...)` MAY also appear in explicitly declared delayed values, field values, class attributes, and explicit `@api.funkify(...)` method configuration.
- Those helper DSL values MUST participate in member dependency extraction and ordering even when they are not inferred from plain method bodies.

### Error Semantics

#### Deterministic failures in `api.funkify`

- Failure classes:
  - unknown adapter,
  - invalid sub type,
  - callable `fn` kwarg collision,
  - sub-cycle detection,
  - adapter runnable-resolution failures,
  - invalid `resolve_runnable` return type or shape.
- Retryable or non-retryable: non-retryable until the declaration or adapter configuration is corrected.
- Transient vs terminal: terminal for that declaration/materialization path.
- Required caller behavior: correct the delayed-runnable declaration or selected adapter configuration.
- Required operator action: repair adapter registration or runnable-lowering implementation when the failure is due to adapter resolution behavior.

#### Deterministic failures in dagclass compilation

- Failure classes:
  - dependency cycles,
  - unknown dependency references,
  - unsupported dynamic or self-reflective constructs,
  - unknown local member references discovered during member dependency extraction,
  - self-referential local member cycles,
  - reserved-name violations.
- Retryable or non-retryable: non-retryable until the dagclass definition is corrected.
- Transient vs terminal: terminal and MUST fail before execution.
- Required caller behavior: fix the class definition, dependency structure, or helper usage.
- Required operator action: none beyond correcting author code or compiler implementation defects.

#### Deterministic failures in `api.run`

- Failure classes:
  - instance is not a dagclass instance,
  - resolved entrypoint is missing,
  - resolved entrypoint is not `DelayedRunnable`,
  - compiled dagclass namespace is incomplete or internally inconsistent,
  - invalid run name,
  - argument binding or invocation contract violations.
- Retryable or non-retryable: non-retryable until the invocation shape or compiled dagclass instance is corrected.
- Transient vs terminal: terminal for that run invocation.
- Required caller behavior: pass a valid compiled dagclass instance and a valid invocation shape.
- Required operator action: correct caller code or compiler output if namespace materialization is inconsistent.

### Authority Handoffs

- DAG result-node semantics are authoritative in [../api.md](../api.md).
- `IndexOps` execution and staging semantics are authoritative in [../internal/ops/index-ops.md](../internal/ops/index-ops.md).
- Testing and debug access to the innermost script callable is authoritative in [testing.md](testing.md) via `defunkify(...)`.
- Script runtime prepopulation behavior and executor-specific kwargs/schema details are authoritative in [executor-catalog.md](executor-catalog.md).
- Adapter runtime composition, adapter and executor lifecycle behavior, and supervisor behavior are authoritative in [runtime-contract.md](runtime-contract.md).
- Registry and adapter discovery contracts are authoritative in [registries.md](registries.md).
- Core adapter-boundary payload and output contracts are authoritative in [../adapter-execution-contract.md](../adapter-execution-contract.md).
- Core execution-model and error-model behavior are authoritative in [../execution-model.md](../execution-model.md) and [../errors.md](../errors.md).

## Compatibility

- Backward compatibility guarantees apply to the public interfaces `api.ref`, `api.load`, `api.funkify`, `api.dagclass`, and `api.run`.
- Delayed-action lowering semantics, dagclass dependency extraction rules, inferred `prepop` rules, and run-name derivation rules are part of the specified contrib API contract and MUST remain stable across minor versions.
- Adding new optional decorator parameters or new helper capabilities is forward-compatible only when existing declaration, lowering, dependency, and execution behavior remains unchanged for already-specified inputs.

## References

- [runtime-contract.md](runtime-contract.md)
- [registries.md](registries.md)
- [../adapter-execution-contract.md](../adapter-execution-contract.md)
- [../execution-model.md](../execution-model.md)
- [../errors.md](../errors.md)
- [testing.md](testing.md)
- [executor-catalog.md](executor-catalog.md)
- [../api.md](../api.md)
- [../internal/ops/index-ops.md](../internal/ops/index-ops.md)
