## Why

Revision URI parsing and canonicalization are currently duplicated across configuration and remote ops layers with near-identical rules and slightly different behavior boundaries. That duplication increases drift risk, makes policy changes expensive (like allowing `project.uri` tags), and creates unnecessary coupling between caller intent and URI representation details.

We want one canonical revision URI model and one canonical set of parse/stringify operations, with operation-specific constraints enforced at operation boundaries rather than inside shared parsing logic.

## What Changes

- Introduce one shared revision URI value type (owner, project, branch, tag) with invariant: exactly one of branch/tag is present.
- Introduce one shared parser for revision URIs that returns a fully realized selector (branch or tag), one shared canonical stringifier, and one canonicalize helper (`parse + stringify`).
- Replace duplicated URI parsing/canonicalization implementations in config and remote ops with wrappers/delegation to the shared implementation.
- Update project configuration semantics to allow tag-bearing `project.uri` values.
- Preserve branch-only mutation safety by keeping operation-level branch/tag requirements in mutating remote methods.

## Capabilities

### Modified Capabilities
- `shared-internal-configuration`: project URI normalization/parsing semantics move to centralized shared revision URI utilities and no longer reject tags at config resolution time.
- `remote-project-refs`: remote URI parsing and canonicalization use one centralized implementation while preserving mutable-branch/immutable-tag operation contracts.

## Impact

- Affected code: shared internal config URI helpers, remote ops URI helpers, DmlOps URI assembly sites, and commit revision tracking URI construction.
- Affected behavior: `project.uri` accepts branch or tag selectors; mutation restrictions remain enforced by branch/tag-specific operations.
- User-facing impact: canonical URI behavior stays stable while enabling tag-based project URI configuration and reducing inconsistent URI handling edge cases.
