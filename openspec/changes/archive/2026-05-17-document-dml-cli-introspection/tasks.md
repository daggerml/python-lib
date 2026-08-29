## 1. Add top-level `Dml` introspection metadata

- [x] 1.1 Add a class docstring to `Dml` that explains its role as the shared orchestration boundary.
- [x] 1.2 Add concise method docstrings to the public top-level `Dml` methods describing behavior, constraints, and notable side effects.
- [x] 1.3 Add `typing.Annotated` help metadata to user-facing parameters on the public top-level `Dml` methods, keeping defaults in the signatures.

## 2. Add namespace introspection metadata

- [x] 2.1 Add class docstrings to the public namespace classes reachable from `Dml`, including admin sub-namespaces.
- [x] 2.2 Add concise method docstrings to public namespace methods under `config`, `runtime`, `dag`, and `admin`.
- [x] 2.3 Add `typing.Annotated` help metadata to user-facing parameters on public namespace methods, including concise examples for ambiguous selector or URI inputs.

## 3. Verify the introspection contract

- [x] 3.1 Add or update tests that inspect public `Dml` and namespace docstrings plus `Annotated` metadata with extras included.
- [x] 3.2 Run the relevant test suite and confirm the change preserves existing runtime and CLI behavior while exposing the new introspection metadata.
