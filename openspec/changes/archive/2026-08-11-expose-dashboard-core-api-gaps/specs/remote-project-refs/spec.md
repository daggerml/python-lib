## ADDED Requirements

### Requirement: Branch and tag enumeration SHALL select local, fetched, or endpoint refs
The system SHALL enumerate branches and tags from exactly one source selected independently by `remote` and `dep`. With neither selector it SHALL use local refs; with only `remote` it SHALL use refs at configured `remote.root`; with only `dep` it SHALL use locally fetched refs for that dependency; and with both selectors it SHALL use refs at that dependency's configured endpoint. An unknown dependency or a required but unconfigured endpoint SHALL fail with a descriptive configuration error.

#### Scenario: Local source is selected by default
- **WHEN** branch or tag enumeration omits `remote` and `dep`
- **THEN** only refs in the local branch or tag namespace are returned

#### Scenario: Main endpoint is selected by remote
- **WHEN** branch or tag enumeration sets `remote = True` and omits `dep`
- **THEN** only refs in configured `remote.root` are returned

#### Scenario: Fetched dependency source is selected by dependency
- **WHEN** branch or tag enumeration sets `dep = "models"` and leaves `remote = False`
- **THEN** only locally fetched refs for dependency `models` are returned

#### Scenario: Dependency endpoint is selected by both selectors
- **WHEN** branch or tag enumeration sets `remote = True` and `dep = "models"`
- **THEN** only refs at dependency `models`' configured endpoint are returned

### Requirement: Branch and tag enumeration SHALL preserve exact commit tips
Each enumerated branch or tag record SHALL preserve its ref name and the exact commit ref stored at the selected source. The result sequence SHALL be ordered lexicographically by ref name. Endpoint enumeration SHALL NOT require the selected commit or any reachable object to exist in the local object database. If any selected local, fetched, or endpoint ref is malformed or does not identify a commit, enumeration SHALL fail rather than omit or coerce that ref. The caller-facing item shape is owned by `unified-dml-surface`.

#### Scenario: Fetched ref returns tracked tip
- **WHEN** a locally fetched dependency branch `main` tracks `commit:a1`
- **THEN** the enumerated record for `main` carries exact tip `commit:a1`

#### Scenario: Unmaterialized remote tip remains visible
- **WHEN** an endpoint branch `main` points to `commit:b2` and that commit is absent locally
- **THEN** the endpoint record for `main` carries exact tip `commit:b2`

#### Scenario: Results are ordered by name
- **WHEN** the selected source contains refs `zeta`, `main`, and `alpha`
- **THEN** enumeration returns list items in `alpha`, `main`, `zeta` name order

#### Scenario: Malformed local or fetched pointer fails the listing
- **WHEN** a selected local or fetched tracking file does not contain a valid commit pointer
- **THEN** enumeration fails without returning a partial result

#### Scenario: Malformed endpoint ref fails the listing
- **WHEN** a selected endpoint ref contains an invalid typed ref payload
- **THEN** enumeration fails without returning a partial result

#### Scenario: Non-commit endpoint ref fails the listing
- **WHEN** a selected endpoint branch or tag points to a namespace other than `commit`
- **THEN** enumeration fails without materializing that object or returning a partial result

### Requirement: Endpoint ref enumeration SHALL be bounded and read-only
Endpoint branch and tag enumeration SHALL inspect the selected endpoint's descriptor and requested ref namespace. When the descriptor is absent, it SHALL perform one endpoint-state existence listing limited to at most one key anywhere under the resolved endpoint root solely to distinguish a truly empty endpoint from incompatible non-empty state; the probe SHALL NOT enumerate or decode object payloads. Enumeration SHALL NOT fetch or materialize CAS objects, mutate local tracking refs, create an endpoint descriptor, or otherwise write local or remote state.

#### Scenario: Listing does not materialize remote commits
- **WHEN** endpoint enumeration observes a ref whose commit is absent locally
- **THEN** the commit remains absent from the local object database after enumeration

#### Scenario: Listing does not update tracking refs
- **WHEN** an endpoint ref differs from its local tracking ref
- **THEN** endpoint enumeration returns the endpoint tip without changing the local tracking ref

#### Scenario: Listing an empty uninitialized endpoint is non-mutating
- **WHEN** endpoint enumeration targets an empty endpoint with no descriptor
- **THEN** it returns no refs and does not create a descriptor

#### Scenario: Descriptorless emptiness check is bounded
- **WHEN** endpoint enumeration finds no descriptor
- **THEN** it performs one existence listing limited to at most one key anywhere under the resolved endpoint root
- **AND** it does not enumerate object payloads or traverse CAS

#### Scenario: Listing rejects incompatible endpoint state without mutation
- **WHEN** endpoint enumeration targets a non-empty endpoint with a missing, legacy, or unsupported descriptor
- **THEN** it fails without reading project refs or changing the endpoint
