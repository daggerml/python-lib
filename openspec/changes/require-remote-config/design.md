## Context

Remote-backed runtime behavior already depends on a real remote root, but some constructors and helpers still expose remote configuration as optional in their signatures and defaults. That creates a split between the actual runtime contract and the type-level/API contract, which in turn encourages invalid test fixtures, dead fallback branches, and avoidable optional handling in remote-aware code.

This change is cross-cutting because the contract is expressed in multiple places: remote-aware ops classes, execution helpers, runtime/config call sites, and test adapter scripts that sometimes use a remote-aware type only to borrow a transaction wrapper.

## Goals / Non-Goals

**Goals:**
- Make remote configuration required in every remote-aware constructor and helper signature.
- Remove optional typing and `None` defaults for remote-root and equivalent remote config parameters.
- Align tests and helper code with the required-remote contract.
- Keep purely local setup code on local-only primitives instead of remote-aware ops.

**Non-Goals:**
- Changing remote protocol behavior or storage layout.
- Adding new runtime validation beyond the existing remote-root parsing/usage logic.
- Redesigning non-remote APIs that do not depend on remote state.

## Decisions

### Require remote config at the signature boundary
Remote-aware components will require explicit remote configuration in their public constructors and helpers rather than modeling it as optional. This matches the real operational contract and lets type checking enforce the same rule the runtime already depends on.

Alternative considered: leave optional signatures and rely on runtime errors.
Why not: it preserves misleading APIs and keeps unnecessary optional branches throughout the codebase.

### Fix local-only helpers by using local-only primitives
Tests or scripts that only need transaction access will switch from remote-aware ops classes to `BaseOps` or other local-only primitives. This keeps the required-remote contract intact without inventing fake remote defaults for code paths that are not actually remote-backed.

Alternative considered: pass placeholder remote roots everywhere.
Why not: it hides the distinction between remote-aware and local-only flows and weakens the contract we are trying to make explicit.

### Remove optional remote typing without adding compatibility shims
The implementation will directly remove `Optional`, `| None`, and `None` defaults for remote configuration fields and parameters. Callers must be updated in the same change.

Alternative considered: temporary overloads or compatibility wrappers.
Why not: they prolong an unsupported contract and add cleanup work for no product value.

## Risks / Trade-offs

- [Call sites missed during the sweep] -> Use type checking and the full test suite to catch remaining bare constructors or optional remote call paths.
- [Tests depended on borrowing remote-aware ops for local setup] -> Move those helpers to `BaseOps` or equivalent local-only primitives.
- [Broader API tightening may surface more compile-time churn] -> Keep the change scoped to remote-aware surfaces and update all in-repo callers together.
