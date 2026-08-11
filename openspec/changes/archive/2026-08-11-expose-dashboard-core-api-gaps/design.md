## Context

See `proposal.md` for motivation. The existing core already persists launch state and can read its resume payload, stores local/fetched refs as lightweight files, stores branch upstream metadata by local branch name, and can enumerate endpoint ref names. The public namespace currently omits launch-state and arbitrary-upstream reads, while branch/tag listing returns names only. Endpoint construction currently validates or initializes the remote descriptor, so the listing path needs an explicitly non-initializing read mode to satisfy the side-effect boundary.

The shared `Dml` surface is the orchestration boundary. Exact database identities in its payloads use `Ref`, and endpoint tips are typed remote refs whose `ref.to` must be a commit ref even when that commit is not materialized locally.

## Goals / Non-Goals

**Goals:**

- Reuse existing launch-state and upstream storage without changing persisted formats.
- Route all four branch/tag source combinations through one explicit source-selection path.
- Return lexicographically ordered `{"name": str, "commit": Ref}` item lists with exact source tips and fail closed on invalid selected refs.
- Keep endpoint enumeration read-only and independent of local CAS availability.

**Non-Goals:**

- Fetching, commit inspection, tracking-ref refresh, or CAS materialization during listing.
- Adding a separate fetched-ref namespace or public listing API.
- Adding tag upstreams, dependency upstreams, pagination controls, or ref filtering.
- Preserving the old `list[str]` return shape for branch and tag listings.

## Decisions

### Delegate launch-state reads through the existing execution-state reader

`_RuntimeNamespace.read_launch_state(execution_id)` will delegate to the existing execution-state operation and return its `dict | None` result. This deliberately exposes the JSON-decoded executor `resume_state`, not the enclosing storage record, execution lifecycle record, or graph projection.

Alternative considered: return the full launch-state envelope. That would expose coordination metadata not requested by inspection clients and diverge from the established core reader's semantics.

### Centralize ref source selection at the shared namespace boundary

Branch and tag `list` methods will share the same source matrix and differ only by ref kind. Local and fetched sources will pair each enumerated name with the corresponding `Head` ref reader. Endpoint sources will resolve either `remote.root` or the selected dependency root and use a read-only remote ref enumeration operation. The shared result builder emits one `RefListItem` per ref with exact shape `{"name": str, "commit": Ref}` and sorts items by `name`.

Alternative considered: add `refs.list_fetched()` or separate list methods per source. This duplicates namespace behavior and makes source combinations harder for callers to compose.

### Treat `remote` and `dep` as independent only for ref listing

For branch/tag listing, `dep` chooses the dependency endpoint and `remote` chooses endpoint state instead of fetched local state. This is an intentional exception to revision-consuming methods where `remote` and `dep` are mutually exclusive, because listing has a four-source matrix rather than one revision-resolution mode.

Alternative considered: reject both selectors and introduce another flag. That cannot express dependency endpoint listing with the requested API shape.

### Add a non-initializing endpoint inspection path

Remote ref enumeration will validate a present descriptor but will not create one. If the descriptor is absent, one existence listing limited to at most one key anywhere under the resolved endpoint root distinguishes a truly empty endpoint from incompatible state without decoding or traversing objects. A missing descriptor on an empty endpoint yields an empty listing; a missing descriptor on a non-empty endpoint and legacy or unsupported descriptors continue to fail closed. The operation otherwise lists only the requested branch or tag ref namespace from the canonical endpoint layout and decodes each typed ref's `ref.to` as a commit `Ref`; it does not invoke object materialization or tracking updates.

Alternative considered: reuse normal remote construction unchanged. That can initialize an empty endpoint descriptor, violating the read-only contract.

### Expose upstream metadata without duplicating it

`dml.branch.get_upstream(branch)` will directly expose the existing validated `{branch: str}` upstream payload or `None`. Valid names without an association, including names without a current local ref, return `None`; invalid names and malformed metadata preserve existing validation failures. No new metadata is stored and no analogous tag method is introduced.

Alternative considered: extend `status()` with an arbitrary branch parameter. This overloads checkout status and does not fit ref-oriented clients.

## Risks / Trade-offs

- [Breaking branch/tag listing payloads can affect current callers] -> Mark the change as breaking, update all internal callers/tests/docs in one implementation, and do not maintain dual return shapes.
- [A large endpoint can produce a large item list] -> Bound reads to one ref-kind prefix and rely on the storage client's paginated listing; do not traverse CAS or unrelated endpoint keys.
- [Remote refs can be malformed or point outside the commit namespace] -> Validate typed ref payloads and commit namespace before including a result, failing closed on invalid endpoint data.
- [Read-only descriptor handling can drift from mutating remote validation] -> Share descriptor decoding/version checks while separating only the empty-root initialization behavior.

## Migration Plan

1. Add the read-only endpoint enumeration primitive and source-to-tip helpers with contract tests.
2. Change branch/tag public listings and update all repository callers, tests, generated-help expectations, and docs to consume structured item lists.
3. Add the two direct inspection methods and their tests/docs.
4. Run focused core contracts followed by the repository's required lint, type, and non-slow test checks.

Rollback reverts the public methods and read-only enumeration path together. No stored data requires migration or rollback.
