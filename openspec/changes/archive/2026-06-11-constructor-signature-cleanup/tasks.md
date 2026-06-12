## 1. Constructor Surface

- [x] 1.1 Expand `Dml.__init__` to accept the full supported config-var surface via Python-friendly parameter names.
- [x] 1.2 Align `Dml.init(...)` to accept the same config kwargs plus init-only args.

## 2. Config-Dict Factory

- [x] 2.1 Add `Dml.from_config_vars(...)` for flattened canonical config-var dictionaries.
- [x] 2.2 Route constructor and factory inputs through one normalization path.

## 3. Tests And Docs

- [x] 3.1 Update Python API and CLI constructor-generation tests for the new signatures.
- [x] 3.2 Update configuration and Python API docs to describe the two supported construction shapes.
