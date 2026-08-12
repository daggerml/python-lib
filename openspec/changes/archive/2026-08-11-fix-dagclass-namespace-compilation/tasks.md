## 1. Namespace Compilation Contracts

- [x] 1.1 Add contract coverage proving direct use of a compiled dagclass method captures constructor attributes when the caller DAG has colliding names and when those names are absent.
- [x] 1.2 Add contract coverage for transitive method dependencies, nested dagclass members, and `api.ref` values inside externally defined funkify wrappers adopted by a dagclass.
- [x] 1.3 Add contract coverage proving unknown or unavailable dagclass-local references and member dependency cycles fail during instantiation.
- [x] 1.4 Add contract coverage proving `api.run()` executes an already compiled entrypoint and rejects uncompiled dagclass-marked objects.
- [x] 1.5 Add contract coverage proving a funkify-decorated method binds both method-body dependencies and wrapper-level `api.ref` values to the same dagclass namespace.

## 2. Incremental Namespace Compiler

- [x] 2.1 Refactor dagclass dependency discovery so evaluated attributes seed member map `M` and all remaining members are ordered before namespace binding.
- [x] 2.2 Implement one recursive binder for delayed references across `DelayedRunnable`, concrete `Runnable`, their `sub` and `kwargs`, and supported containers; resolve only through the current `M` and report unavailable names as compilation errors.
- [x] 2.3 Compile members in topological order, bind each member against `M`, then add the resolved member to `M` and expose that resolved value on the instance.
- [x] 2.4 Preserve cycle detection, reserved-name validation, nested dagclass composition, and script worker self-containment while removing paths that allow unresolved dagclass references to escape.

## 3. Entrypoint Execution And Documentation

- [x] 3.1 Update `api.run()` as needed so it only validates and executes the compiled entrypoint without recompiling or resolving references against its runtime DAG.
- [x] 3.2 Update dagclass authoring documentation to define the self-contained namespace, instantiation-time compilation, constructor-based external injection, and the external-funk `api.ref` sharp edge.
- [x] 3.3 Update relevant examples or regression fixtures so caller-DAG name collisions visibly produce values from the dagclass instance.
- [x] 3.4 Add a Docker-backed example whose decorated dagclass entrypoint binds executor configuration and method-body references within one namespace.

## 4. Verification

- [x] 4.1 Run focused contrib dagclass and delayed-action contract tests, including a Moto-backed end-to-end reproduction of the caller-name collision.
- [x] 4.2 Run required type checking, lint-fix, and non-slow test commands from the repository finish-check workflow.
- [x] 4.3 Run strict OpenSpec validation for `fix-dagclass-namespace-compilation` and review the final implementation diff for scope and documentation consistency.
