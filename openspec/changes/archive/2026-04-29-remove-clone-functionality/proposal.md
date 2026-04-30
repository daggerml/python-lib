## Why

The current `clone` surface duplicates initialization and project bootstrap pathways, increasing maintenance cost and creating split behavior between CLI routing and internal orchestration. Removing clone now simplifies the product model to an init-first workflow and enforces the intended architecture where CLI commands are thin adapters over `daggerml._internal` public APIs.

## What Changes

- **BREAKING**: Remove the `dml clone` command from the CLI and all internal clone orchestration paths.
- **BREAKING**: Remove `clone`-specific behavior from `DmlOps` and internal ops/contracts; do not keep compatibility shims or aliases.
- Preserve and harden `init` as the only project bootstrap entrypoint.
- Refactor CLI project commands to remain thin wrappers that delegate directly to supported `daggerml._internal` APIs with no embedded workflow logic.
- Remove dead code, tests, docs references, and configuration/hook branches that only exist for clone flows.

## Capabilities

### New Capabilities
None.

### Modified Capabilities
- `thin-cli-routing`: remove clone delegation requirements and strengthen requirement that CLI project commands are thin wrappers over internal APIs only.
- `git-like-commit-ops`: remove clone composition requirements (`fetch` then `checkout`) and unsupported clone-target semantics.
- `remote-project-refs`: remove clone-related initialization/origin recording requirements while preserving init and non-clone remote workflows.

## Impact

- Affected code: `src/daggerml/_cli/**`, `src/daggerml/api.py` (`DmlOps`), and internal ops modules supporting clone orchestration.
- Affected tests: CLI and internal operation tests that cover clone behavior need removal or rewrite toward init+fetch/checkout workflows.
- Affected docs/specs: clone references in OpenSpec capabilities and user-facing CLI docs must be removed.
- User impact: existing clone-based workflows are no longer available; users must initialize projects with `init` and use explicit fetch/checkout/pull flows.
