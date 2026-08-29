## Context

The codebase currently has two URI models and two parse/canonicalization implementations:

- `parse_dml_project_uri` / `normalize_project_uri` in `_internal.config`
- `parse_dml_uri` / `canonical_dml_uri` in `_internal.ops.remote`

Both encode the same grammar (`dml://owner/project#branch` or `@tag`) and many of the same validations. This creates avoidable duplication and policy drift.

At the same time, operation constraints differ by context:

- configuration should accept branch or tag
- branch push requires branch
- tag push requires tag

Those operation constraints should be applied at call boundaries, not baked into the shared parser.

## Goals / Non-Goals

**Goals:**
- Establish one shared revision URI model and parse/stringify surface.
- Ensure canonical stringification always emits branch or tag form.
- Allow `remote.project` to carry either a branch or a tag.
- Keep mutation restrictions explicit in operation methods.

**Non-Goals:**
- Redesigning commit revision expression grammar (`HEAD`, `~N`, `origin/main`).
- Changing remote ref namespace/layout.
- Changing branch/tag mutability semantics.

## Decisions

- Single shared revision URI value type with XOR invariant.
  - Decision: represent revision URI as `RevisionUri(owner, project, branch, tag)` with exactly one non-`None` among `branch`, `tag`.
  - Rationale: type-level explicitness prevents ambiguous states and simplifies canonicalization.

- Central parser returns fully realized revision selectors.
  - Decision: parser validates URI structure/segments and invariant requirements, and resolves missing selector to a branch via provided default-branch input so parsed `RevisionUri` always has exactly one selector set.
  - Rationale: parsing is the single realization boundary; downstream code receives a complete typed revision object.

- Central stringifier is canonical and total for valid `RevisionUri` values.
  - Decision: stringifier always emits canonical `dml://owner/project#branch` or `dml://owner/project@tag`.
  - Rationale: one path for all generated URI text removes ad-hoc interpolation drift.

- Canonicalize helper composes parse + stringify.
  - Decision: provide explicit helper for canonical URI normalization.
  - Rationale: most call sites want normalization without manual two-step calls.

- Operation-level constraints remain where behavior differs.
  - Decision: keep branch/tag requirements at operation boundaries (e.g., push branch/tag methods).
  - Rationale: this preserves existing behavior contracts while enabling broader URI acceptance in configuration.

## Migration Plan

1. Add shared `RevisionUri` parse/stringify/canonicalize utilities.
2. Convert existing config and remote helper APIs into wrappers over shared utilities.
3. Migrate URI assembly call sites in `DmlOps` and commit tracking URI construction to shared stringifier.
4. Remove branch-only rejections in config resolution and project config loading.
5. Keep or strengthen explicit branch/tag assertions in mutating remote ops.
6. Update tests/spec expectations for tag-accepting project URIs and centralized helper behavior.

## Risks / Trade-offs

- [Backward compatibility surprises in config flows] -> Mitigation: preserve canonical string output and migrate with wrapper compatibility first.
- [Over-centralizing policy] -> Mitigation: keep parser policy-neutral about selector-type capabilities and enforce behavior-specific branch/tag rules at operation boundaries.
- [Test churn] -> Mitigation: sequence migration via wrappers to keep external behavior stable while internals consolidate.

## Open Questions

- Should `DmlProjectConfig` become revision-shaped (branch/tag) or remain branch-oriented with conversion behavior at load/save boundaries?
