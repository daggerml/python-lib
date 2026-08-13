## 1. Public API Behavior

- [x] 1.1 Update `Dag.result` to hydrate and raise a committed DAG's terminal error ref before resolving its result node.
- [x] 1.2 Preserve the existing successful-result return and missing-terminal-state repository error behavior.

## 2. Verification

- [x] 2.1 Add an API contract test proving failed committed DAG result access raises the hydrated stored `Error` and calls the public error query with the terminal error ref.
- [x] 2.2 Add an API integration test that commits or loads a failed DAG and proves `.result` raises the persisted error fields.
- [x] 2.3 Run the focused API DAG contract and integration test suites.

## 3. Documentation

- [x] 3.1 Document successful, failed, and terminal-less `Dag.result` behavior in the public Python authoring reference and error guidance.
