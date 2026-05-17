## 1. Establish shared revision URI boundary

- [x] 1.1 Add shared `RevisionUri` value type with XOR invariant (`branch` xor `tag`).
- [x] 1.2 Add shared parse utility for `dml://owner/project#branch|@tag` that returns fully realized `RevisionUri` (injecting default branch when selector omitted).
- [x] 1.3 Add shared stringify utility that emits canonical branch/tag URI form.
- [x] 1.4 Add shared canonicalize helper (`parse + stringify`) and apply 64-byte canonical URI validation in one place.

## 2. Migrate existing helpers and call sites

- [x] 2.1 Convert `_internal.config` URI helpers to delegate to shared revision URI utilities.
- [x] 2.2 Convert `RemoteOps` URI helpers to delegate to shared revision URI utilities.
- [x] 2.3 Replace ad-hoc URI interpolation in `DmlOps` project remote URI construction with shared stringify.
- [x] 2.4 Replace commit tracking URI interpolation with shared stringify where DML URI tracking heads are created/looked up.

## 3. Align policy semantics (tags allowed in project URI)

- [x] 3.1 Remove config-layer branch-only rejection for `remote.project`.
- [x] 3.2 Keep operation-level branch/tag capability checks (e.g., push-branch requires branch; push-tag requires tag).
- [x] 3.3 Review and update `DmlProjectConfig` behavior to support tag-bearing project URI usage without breaking branch mutation flows.

## 4. Validate behavior and documentation

- [x] 4.1 Update/add tests for centralized parse/stringify/canonicalize behavior and wrapper compatibility.
- [x] 4.2 Update/add tests proving `remote.project` accepts tags while mutation constraints remain enforced by operation methods.
- [x] 4.3 Update relevant OpenSpec spec deltas (`shared-internal-configuration`, `remote-project-refs`) to reflect centralized URI handling and tag-allowed project URI semantics.
