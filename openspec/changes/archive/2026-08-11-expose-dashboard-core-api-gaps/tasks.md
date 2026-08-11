## 1. Read-Only Ref Enumeration Primitives

- [x] 1.1 Add local and fetched ref readers that produce lexicographically ordered name/tip records, including dependency tracking refs, while preserving exact commit `Ref` identity and existing invalid-ref failures; leave public `RefListItem` shaping to the shared namespace task.
- [x] 1.2 Add a non-initializing remote inspection path that validates present descriptors; when absent, performs one existence listing limited to at most one key anywhere under the resolved endpoint root; treats a truly empty endpoint as empty; rejects incompatible non-empty endpoints; and lists only typed commit refs from one requested branch/tag namespace without CAS materialization or writes.
- [x] 1.3 Add focused core contract tests proving exact tips are returned, an unmaterialized endpoint commit remains visible, malformed/non-commit remote refs fail closed, and endpoint listing creates no descriptor, tracking ref, or local CAS object.

## 2. Public Branch And Tag Listings

- [x] 2.1 Define `RefListItem` as a `TypedDict` with exact fields `name: str` and `commit: Ref`, then change `dml.branch.list(*, remote: bool = False, dep: str | None = None) -> list[RefListItem]` to implement the local, main-endpoint, fetched-dependency, and dependency-endpoint source matrix without applying revision methods' `remote`/`dep` mutual-exclusion rule.
- [x] 2.2 Change `dml.tag.list(*, remote: bool = False, dep: str | None = None) -> list[RefListItem]` to use the same four-source behavior and exact-tip item contract as branch listing.
- [x] 2.3 Update branch/tag contract tests and all existing callers for the structured item-list return shape; cover empty sources, unknown dependencies, missing required `remote.root`, both selectors together, exact item keys and types, and deterministic name ordering for branches and tags.

## 3. Direct Inspection Methods

- [x] 3.1 Add `dml.runtime.read_launch_state(execution_id: str) -> dict | None`, delegating to the execution-state reader so a JSON-object executor resume state is returned unchanged, missing state returns `None`, and scalar/array/`null` resume state fails as malformed; add runtime namespace contract tests for these outcomes and exclusion of execution-record fields.
- [x] 3.2 Add `dml.branch.get_upstream(branch: str) -> UpstreamInfo | None`, where `UpstreamInfo` has exact shape `{branch: str}`, delegating to existing validated branch metadata without depending on the current checkout; add tests for current and non-current branches, valid unknown/unconfigured names returning `None`, invalid names, malformed metadata raising `DmlRepoError`, and absence of a tag counterpart.
- [x] 3.3 Update public namespace docstrings and `Annotated` parameter metadata, then update generated CLI/introspection contract expectations so revision-source commands remain mutually exclusive while `branch list` and `tag list` accept `--remote` with `--dep`.

## 4. Documentation And Validation

- [x] 4.1 Update `docs/use/reference/python-authoring.md`, `docs/use/reference/cli.md`, history/remotes, execution/runtime, and error-facing docs to describe launch-state reads, branch-only upstream lookup, the four listing source combinations, generated CLI flags, exact-tip list items, remote read-only behavior, and the breaking `list[str]`-to-`list[RefListItem]` migration.
- [x] 4.2 Run focused new and changed core contract tests, then complete the required validation sequence in order: `uv run --dev pyright`, `uv run --dev ruff check --fix .`, and `uv run --dev pytest -m 'not slow' .`; review any Ruff edits and require all checks to pass.
