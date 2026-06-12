## 1. Clone Workflow

- [x] 1.1 Add `Dml.clone(...)` as a bootstrap classmethod parallel to `Dml.init(...)`.
- [x] 1.2 Persist branchless `remote.project` and initialize the local repo before fetch/checkout steps.

## 2. Ref Selection

- [x] 2.1 Default bare project URIs to `default.branch_name`.
- [x] 2.2 Attach HEAD for branch clones and detach HEAD for tag clones.

## 3. Tests And Docs

- [x] 3.1 Add contract coverage for bare-project, branch, and tag clone flows.
- [x] 3.2 Update Python API and configuration docs to describe clone bootstrap behavior.
