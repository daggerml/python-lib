## 1. Align init contract and config resolution

- [x] 1.1 Update `DmlOps.init` inputs/signature to remove directory-placement (`here`) behavior and define in-place `.dml/` initialization contract.
- [x] 1.2 Route init-time options through the shared internal config resolver and validate resolved canonical fields before filesystem mutation.
- [x] 1.3 Ensure required init-time values fail fast with explicit errors when unresolved/invalid (including `remote.project` and `remote.uri` when required by bootstrap flow).

## 2. Implement init recovery bootstrap behavior

- [x] 2.1 Add initialization path for `.dml/config.toml` present + `.dml/db/` missing that creates the missing DB state idempotently.
- [x] 2.2 Trigger pull during recovery when resolved config includes `remote.project`, using resolved remote/project context.
- [x] 2.3 Ensure recovery path skips pull when no `remote.project` is configured and still completes local init successfully.

## 3. Update callers, docs, and test coverage

- [x] 3.1 Update CLI/API entrypoints and help/error text to match in-place init semantics and resolver-validated options.
- [x] 3.2 Add/adjust tests for local-only init location semantics, strict config validation failures, and recovery-mode pull/no-pull branches.
- [x] 3.3 Add regression tests for repeat init idempotency across clean, fully initialized, and config-only partial states.
