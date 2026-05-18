## Implementation Review

### Gaps and Deviations

**GAP 1 — Dead `current_branch` parameter on `CommitOps.resolve_revision`**

`resolve_revision(value, *, current_branch: str | None = None, project_dir: str = ".")` accepts `current_branch` but never uses it. HEAD always resolves through `get_head_state()`. This is the spec's explicitly-rejected "injected branch context" pattern still present as dead API surface. Any future caller could assume it works. Should be removed.

**GAP 2 — Branch identifier regex too narrow**

`HeadOps._IDENTIFIER_RE = re.compile(r"^[A-Za-z0-9\-\*\|_]+$")` rejects `/`, `.`, and other characters that are valid in ref names elsewhere in the codebase. Branch names like `feature/my-thing` or `v1.0.0` would be accepted by `create_branch` but then rejected when trying to write them to `HEAD` via `write_attached_head`. Silent, inconsistent narrowing not specified in the proposal.

**GAP 3 — CLI help text contradicts the spec**

`setup_init_parser` still shows `dml init --remote-project dml://alice/my-project#main` as an example. This is exactly the form the spec forbids. Validation rejects it at runtime, but the help text is misleading.

**GAP 4 — `Dml.temporary()` overuses the branch override**

`Dml.temporary()` passes `branch=branch` to `Dml(...)`, causing `_runtime_branch()` to always return the literal string rather than consulting `.dml/HEAD`. If a subsequent `checkout_project("HEAD~1")` moves HEAD to detached state, `self.branch` becomes stale. The design specifies `Dml(branch=...)` as a deliberate override, not a convenience shortcut — `temporary()` uses it as the latter with no documentation to clarify.

**GAP 5 — `_default_project_branch` has a config-derived branch fallback**

`_default_project_branch(branch)` resolves as `branch or attached_head or config.default_branch`. The third arm reaches `DmlConfig.default_branch`. The design does carve out `default_branch` for bootstrap/fetch scenarios, but this method is used in `_project_remote_root` for both `fetch_project` and `pull_project` remote URI construction with no documentation distinguishing the carve-out case from a mutable workflow context.

**MINOR — `_project_home()` has a silent temp-dir fallback**

If `_db.path` is not under a `.dml/db` layout, `_project_home()` silently falls back to a temp directory. The spec's "fails closed" stance calls for a hard error — a silent fallback means two `HeadOps` instances for the same repo could write HEAD to different paths.

---

### Summary

| Issue | Severity |
|---|---|
| Dead `current_branch` parameter on `resolve_revision` | Should be removed |
| Branch identifier regex too narrow — rejects `/`, `.` | Bug — inconsistent with rest of codebase |
| CLI help text shows branch-qualified URI example | Contradicts spec |
| `Dml.temporary()` sets `self.branch`, can go stale after checkout | Design deviation |
| `_default_project_branch` config fallback undocumented | Needs clarification |
| `_project_home()` temp-dir fallback | Contradicts "fails closed" stance |

---

### Follow-up Status

| Issue | Status | Resolution |
|---|---|---|
| Dead `current_branch` parameter on `resolve_revision` | Fixed | Removed `current_branch` from `CommitOps.resolve_revision` and `resolve_revision_ref`, and updated callers/tests. |
| Branch identifier regex too narrow — rejects `/`, `.` | Fixed | HEAD-attached branch validation now uses repo ref-name rules, and nested branch names are covered in tests. |
| CLI help text shows branch-qualified URI example | Fixed | `dml init` examples/help now use branchless `dml://owner/project` URIs. |
| `Dml.temporary()` sets `self.branch`, can go stale after checkout | Fixed | `Dml.temporary()` now returns a HEAD-driven runtime by default instead of pinning a branch override. |
| `_default_project_branch` config fallback undocumented | Not changed | Behavior remains intentional for fetch/bootstrap-style remote URI defaults; mutable workflows still require attached HEAD or explicit branch. |
| `_project_home()` temp-dir fallback | Fixed | `HeadOps._project_home()` now fails closed when the DB path is not a real `.dml/db` layout, and affected fixtures/scripts were updated. |
