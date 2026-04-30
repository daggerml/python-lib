## REMOVED Requirements

### Requirement: Clone composes fetch then checkout
**Reason**: Clone is intentionally removed with no backward compatibility; bootstrap behavior is now modeled through explicit init and subsequent git-like commands.
**Migration**: Use `dml init` to create local project state, then run `dml fetch` and `dml checkout` (or `dml pull`) explicitly.
