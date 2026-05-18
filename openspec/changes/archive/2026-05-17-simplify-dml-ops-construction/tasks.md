## 1. Remove indirection layers

- [x] 1.1 Delete the `DmlOps` facade from `src/daggerml/_internal/ops/__init__.py` and remove code that imports or constructs it.
- [x] 1.2 Delete `_OpsProxy`, `call_ops_method`, and related string-dispatch helpers from `src/daggerml/_internal/dml.py`.

## 2. Rebuild direct ops construction

- [x] 2.1 Rewrite the module-level ops helper functions in `src/daggerml/_internal/dml.py` so they open the DB and instantiate the owning concrete ops classes directly.
- [x] 2.2 Preserve existing remote-aware behavior by threading resolved `remote.root` and fetch-worker configuration through the direct helper construction path.
- [x] 2.3 Keep repository bootstrap behavior intact by replacing `DmlOps.create(...)` usage with direct DB/bootstrap orchestration in `daggerml._internal.dml`.

## 3. Realign tests and docs

- [x] 3.1 Update contract tests that import or describe `DmlOps` so they validate the direct helper-based construction path instead.
- [x] 3.2 Update docs and OpenSpec-linked prose that still describe `DmlOps` as an active internal facade or default-runtime boundary.

## 4. Verify simplified boundary

- [x] 4.1 Run the targeted contract and unit tests covering `daggerml._internal.dml`, bootstrap, and remote-aware helper construction.
- [x] 4.2 Confirm the surviving `Dml` and namespace surface is unchanged and no backward-compatibility shims remain.
