## 1. Public API Implementation

- [x] 1.1 Add the optional `list[str] | None` `tags` field with a `None` default to `daggerml.api.Dag`.
- [x] 1.2 Update `Dag.commit()` to add provided tags in order after a successful runtime commit, while preserving committed wrapper state and propagating tag errors.

## 2. Contract Coverage

- [x] 2.1 Add focused API contract tests for multiple tags, omitted tags, and an empty tag list.
- [x] 2.2 Add contract tests proving commit failures skip tag mutation and post-commit tag failures propagate without restoring the runtime token.

## 3. Documentation And Verification

- [x] 3.1 Update the Python authoring reference to document the `Dag(tags=None)` commit behavior and its non-atomic post-commit tag mutations.
- [x] 3.2 Run formatting/type checks, lint fixes, the focused API contract tests, and the non-slow test suite.
