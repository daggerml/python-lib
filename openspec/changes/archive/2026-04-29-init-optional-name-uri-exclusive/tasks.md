## 1. Init identity contract updates

- [x] 1.1 Update `DmlOps.init` validation so `name` and `remote_project` are mutually exclusive.
- [x] 1.2 Allow `name` to be omitted when `remote_project` is supplied and preserve explicit URI authority.
- [x] 1.3 Implement name-based `remote_project` derivation via resolved global config user.
- [x] 1.4 Add explicit failure for unresolved user in name-based init with actionable error text.

## 2. CLI and config integration

- [x] 2.1 Align init CLI argument/help semantics with optional `name` and exclusivity rules.
- [x] 2.2 Ensure init command paths surface the new validation/configuration error messages consistently.

## 3. Test coverage

- [x] 3.1 Add/adjust tests for URI-only init without `name`.
- [x] 3.2 Add/adjust tests for name-only init deriving URI from resolved user.
- [x] 3.3 Add/adjust tests for rejection when both `name` and `remote_project` are provided.
- [x] 3.4 Add/adjust tests for rejection when name-based init cannot resolve global config user.
