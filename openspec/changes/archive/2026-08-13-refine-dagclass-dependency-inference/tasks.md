## 1. Compiler Semantics

- [x] 1.1 Replace the flow-sensitive dagclass analyzer in `src/daggerml/contrib/api.py` with independent direct-attribute reference and assignment collection, compute final dependencies as ordered references minus method-wide assignments and worker-`Dag` reserved names, ignore item access, and validate only final edge endpoints against the dagclass member collection.
- [x] 1.2 Derive reserved names from public `daggerml.api.Dag` dataclass fields and public type attributes, remove `dag` from the reserved set, retain reserved class-member rejection, and reject direct assignment to reserved names because it cannot create a named node.
- [x] 1.3 Expand `tests/contrib/contracts/test_dagclass_script_equivalence_contract.py` with fast contract cases for `self.put`, representative `Dag` fields/properties/methods, non-reserved `dag`, undeclared and declared assignments, assignment on partial or later control-flow paths, unknown final edges, reserved assignments, and complete item-access opacity; assert exact inferred `prepop` dependencies or compilation errors for each case.

## 2. Runtime Behavior

- [x] 2.1 Add focused contrib integration coverage showing that a compiled method executes `self.put` as `Dag.put`, that `self.foo = value` followed by `self.foo` creates and reads an invocation-local named node without a class declaration, and that a read occurring before a syntactically detected assignment retains no inferred dependency and fails through normal missing-node behavior.

## 3. User Documentation

- [x] 3.1 Expand the dagclass section of `docs/use/guides/author-a-dag.md` to explain that script execution binds `self` to `daggerml.api.Dag`, enumerate the current reserved collision names, distinguish reserved lookup behavior from supported functionality, and state that `dag` is not reserved.
- [x] 3.2 Add prominent sharp-bit examples documenting method-wide assignment exclusion without control-flow or ordering analysis, invocation-local `self.foo = value` named nodes, unknown-edge compilation failures, and complete compiler blindness to `self[...]`, `getattr`, `setattr`, aliases, and other dynamic access.
- [x] 3.3 Add the dagclass control-flow and item-access limitations to `docs/sharp-bits-and-security.md`, including the runtime consequence of method-wide assignment exclusion and a link to the complete authoring guidance.

## 4. Validation

- [x] 4.1 Run the focused dagclass contract and integration tests, then run `uv run --dev --all-extras ruff check --fix .`, `uv run --dev --all-extras pyright`, and `uv run --dev --all-extras pytest -m "not slow" .`; fix failures attributable to this change.
- [x] 4.2 Run `openspec validate refine-dagclass-dependency-inference --strict` and confirm the proposal, design, delta spec, tasks, implementation, tests, and documentation remain aligned before marking the change complete.
