## Why

`DmlOps.clone` duplicates clone workflow composition that already exists in lower-level operations and creates an extra maintenance surface with little value. Removing it now simplifies the architecture before more clone/fetch/checkout changes land.

## What Changes

- Remove `DmlOps.clone` entrypoints in all forms (including sync/async/wrapper variants) and eliminate all direct call paths.
- Rewire clone command handling to use surviving internal operations directly, without compatibility shims.
- Delete dead code, helpers, and tests that exist only to support `DmlOps.clone`.
- Update tests and docs to reflect the new routing path and removed internal API.
- **BREAKING**: internal `DmlOps.clone` API is removed with no backward compatibility layer.

## Capabilities

### New Capabilities
- None.

### Modified Capabilities
- `thin-cli-routing`: clone CLI routing no longer delegates through a `DmlOps.clone` workflow method and instead composes clone behavior through supported internal operations.

## Impact

- Affected code: clone-related CLI handlers, `DmlOps` class methods, clone workflow helpers, and associated tests.
- Affected APIs: internal Python API surface that referenced `DmlOps.clone`.
- Dependencies/systems: no external dependency additions; behavior remains aligned with existing fetch/checkout and remote project semantics.
