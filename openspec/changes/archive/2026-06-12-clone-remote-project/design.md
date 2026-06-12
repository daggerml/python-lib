## Context

`Dml.clone(...)` should be the analog of `git clone`, not a copy constructor. It depends on unborn-HEAD support so local bootstrap can start without a synthetic initial commit and then materialize the cloned branch ref from fetched remote state.

## Goals / Non-Goals

**Goals:**
- Add a single repo bootstrap entrypoint for cloning remote project refs.
- Default bare project URIs to `default.branch_name`.
- Attach HEAD for branch clones and detach HEAD for tag clones.
- Persist branchless `remote.project` config during clone.

**Non-Goals:**
- Adding named remotes.
- Implicit network fetches for unrelated workflows.
- Redefining `clone` as a copy-construction API.

## Decisions

- `Dml.clone(...)` is a classmethod parallel to `Dml.init(...)`.
  Rationale: clone is bootstrap, not instance mutation.
- Bare `dml://owner/project` imputes `default.branch_name`.
  Rationale: that is the most git-like default.
- Branch clones end attached; tag clones end detached.
  Rationale: branch refs are mutable local targets, tags are commit snapshots.
- Clone persists only branchless `remote.project` in config.
  Rationale: selector state belongs in checkout state, not config.

## Risks / Trade-offs

- [Clone depends on unborn HEAD landing first] -> Keep this proposal layered after the unborn-HEAD change.
- [Branch/tag selector behavior can drift from fetch/pull semantics] -> Reuse existing URI parsing and fetch mechanics where possible.
- [Existing non-empty target directories may need policy] -> Decide and document that behavior during implementation.
