## 1. Landed groundwork

- [x] 1.1 Expand `src/daggerml/_internal/__init__.py` so the planned shared boundary, helper functions, and ops exports can be reached from one `_internal` surface.
- [x] 1.2 Add `src/daggerml/_internal/dml_context.py` to centralize resolved runtime/project context helpers such as config lookup, branch/default selection, project home checks, and recovery helpers.
- [x] 1.3 Start routing selected `_internal` modules through the shared export surface so the future boundary can depend on one import layer.

## 2. Build the shared `Dml` shell

- [ ] 2.1 Add `src/daggerml/_internal/dml.py` with the shared `Dml` shell, storing only `_context` and `_tempdirs` plus context-manager lifecycle methods.
- [ ] 2.2 Add private helper stubs on `Dml` for delegated ops access, selector resolution, runtime branch lookup, and S3 client creation without exposing extra top-level public attributes.

## 3. Add namespace scaffolding in dependency order

- [ ] 3.1 Add the public `ops` namespace exposing exact subsystem objects under `dml.ops.commit`, `head`, `dag`, `node`, `index`, `cache`, `remote`, `gc`, and `config`.
- [ ] 3.2 Add the public `config` namespace with `get`, `set`, and `show`.
- [ ] 3.3 Add the public `runtime` namespace with `create`, `describe`, `put_literal`, `put_import`, `start_fn`, and `commit`.

- [ ] 3.4 Add `src/daggerml/_internal/dml_resolution.py` with the revision and DAG-selector helpers already referenced by `_internal.__init__.py`.
- [ ] 3.5 Add the public `dag` namespace with `list`, `get`, `checkout`, and `delete` on top of `dml_resolution` plus delegated ops.
- [ ] 3.6 Add the public `admin` namespace with `index.list|get|delete`, `cache.invalidate`, `remote.list|gc`, and `gc`.

## 4. Add top-level porcelain workflows

- [ ] 4.1 Implement read-oriented porcelain first: `status`, `log`, `show`, `diff`, and `branch`.
- [ ] 4.2 Implement mutating/sync porcelain next: `checkout`, `fetch`, `pull`, `push`, `merge`, and `revert`.

## 5. Add bootstrap and recovery flows

- [ ] 5.1 Add `Dml.create` and `Dml.temporary` on top of the shared shell and delegated subsystem helpers.
- [ ] 5.2 Put `Dml.init` on the new shared class while preserving the config-first recovery semantics already captured in `dml_context`.

## 6. Finalize `_internal` wiring and verify the narrowed scope

- [ ] 6.1 Wire `_internal.__init__.py` to export only implemented modules and remove the current broken references to missing `_internal.dml` and `_internal.dml_resolution` modules.
- [ ] 6.2 Keep `DmlOps` and other compatibility surfaces untouched unless a minimal `_internal` wiring change is required to make the shared boundary import cleanly.
- [ ] 6.3 Run focused import and contract coverage for `daggerml._internal.Dml`, `Dml.init`, and the delegated namespace surface.
- [ ] 6.4 Verify the corrected public boundary shape: `Dml` stores only `_context` and `_tempdirs`, and exposes exact subsystem objects only under `dml.ops.*`.
- [ ] 6.5 Confirm the remaining implementation only touches a few files under `src/daggerml/_internal/`; if more churn is required, capture that as a follow-up change instead of expanding this one.
