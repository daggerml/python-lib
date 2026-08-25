## Context

See `proposal.md` for motivation. Five contrib modules currently import `daggerml._core.exec_state`: four need only `validate_adapter_response`, while `contrib.adapters` imports response aliases and `ExecutionState`. The response aliases are already exported by `daggerml._core`; execution records are already available through `Dml.runtime.read_execution_record(Ref(...))`; only the validator lacks a facade export.

The nested adapter CLI must continue reading the published result ref and completing nested cleanup before an ephemeral Docker or Batch environment exits. This change therefore preserves the cleanup sequence rather than moving cleanup ownership.

## Goals / Non-Goals

**Goals:**
- Make `daggerml._core` the only supported import boundary into core-owned contracts.
- Remove all core-submodule imports from non-core DaggerML source modules.
- Preserve adapter validation and nested cleanup behavior.
- Prevent future boundary regressions with a source-level architecture contract.

**Non-Goals:**
- Changing adapter request or response schemas.
- Moving nested cleanup from the adapter CLI to wrapper executors.
- Hiding or renaming the `daggerml._core` facade itself.
- Prohibiting focused `_core` tests from importing implementation modules.

## Decisions

### Export the existing validator from `daggerml._core`

Add `validate_adapter_response` to `_core/__init__.py` and `__all__`, then import it directly from `daggerml._core` in contrib executors. This preserves one canonical validator and makes its cross-boundary role explicit.

Alternative: duplicate or move validation into contrib. Rejected because the runtime and extensions must enforce one wire contract, and contrib should not own a core protocol rule.

### Use public runtime inspection instead of `ExecutionState`

Construct a public `Dml` session for the request's remote root, represent the execution as its public runtime `Ref`, and call `Dml.runtime.read_execution_record(...)` to obtain `state.result_ref`. Continue the existing invoke loop followed by cleanup.

Alternative: return immediately after terminal invoke. Rejected because Docker and Batch run nested adapters in ephemeral environments whose nested cleanup must finish before exit.

Alternative: export `ExecutionState`. Rejected because it would expose storage and coordination mechanics rather than preserving the application boundary.

### Enforce imports with an AST-based architecture contract

Inspect imports under `src/daggerml`. Permit `daggerml._core` and its direct exported names from any source module. Permit `_core` submodule imports only when the importing module is itself under `src/daggerml/_core`. Report every offending file and import so failures are actionable.

An AST check is preferred over text matching because it distinguishes the facade from submodules and avoids comments or strings. The contract targets production source; `_core` tests retain direct access to internals for focused verification.

Alternative: rely on review convention. Rejected because the boundary is hard and regressions are easy to introduce through seemingly harmless imports.

## Risks / Trade-offs

- [Public `Dml` construction performs more setup than direct `ExecutionState` construction] -> Keep the session local to the CLI operation and verify nested adapter integration behavior.
- [Static checks can miss dynamic imports] -> Cover normal Python import forms now; dynamic core loading remains unsupported and can be added to the contract if introduced.
- [Exporting the validator increases the supported facade surface] -> Export only the protocol-level function already required by extension implementations, not execution-state classes.

## Migration Plan

1. Add the validator facade export.
2. Migrate contrib imports and runtime inspection without changing operation flow.
3. Add the architecture contract after the source tree complies.
4. Run focused contrib contracts and the full required validation suite.

Rollback consists of reverting the source migration, facade export, and architecture contract together; no persisted data or wire migration is involved.
