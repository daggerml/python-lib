## Purpose

Define generated command placement and behavior for administrative support, cache control, and local or remote garbage collection.

## Requirements

### Requirement: Administrative CLI flows are grouped under `dml admin`
Low-frequency recovery and agent-support commands SHALL remain under `dml admin`. Cache control and local or remote garbage collection SHALL be generated as top-level command surfaces rather than admin commands.

#### Scenario: Admin help contains only remaining administration commands
- **WHEN** a user inspects `dml admin` help
- **THEN** remaining index recovery and agent-support commands appear under `dml admin`
- **AND** cache, remote, and GC commands do not appear there

### Requirement: Admin index list returns indexes with commit info
`dml admin index list` SHALL return every live index together with commit information for the commit each index currently points to.

#### Scenario: Index list includes commit summaries
- **WHEN** a user runs `dml admin index list`
- **THEN** the command returns JSON with an `indexes` field
- **AND** each index entry includes its identifier and commit information for the pointed-to commit

### Requirement: Admin index get returns full index inspection payload
`dml admin index get <index-id>` SHALL return index inspection data including commit information for the commit the index points to, rather than only a commit identifier.

#### Scenario: Index get includes commit details
- **WHEN** a user runs `dml admin index get idx1`
- **THEN** the command returns JSON with an `index` object
- **AND** that object includes commit metadata for the pointed-to commit

### Requirement: Admin index delete removes an index
`dml admin index delete <index-id>` SHALL delete the selected index and report the deletion result as JSON.

#### Scenario: Index delete reports success
- **WHEN** a user runs `dml admin index delete idx1`
- **THEN** the command returns JSON containing `index` and `deleted`

### Requirement: Cache invalidation accepts exact execution refs only
The generated CLI SHALL expose cache invalidation as `dml cache invalidate <execution-ref> [more execution refs]`. It SHALL accept one or more `index:` or `frozenindex:` refs and SHALL NOT accept cache keys, bare execution IDs, DAG refs, argv refs, or other selector types.

#### Scenario: Cache invalidation accepts multiple execution refs
- **WHEN** a user runs `dml cache invalidate index:e1 frozenindex:e2`
- **THEN** the command invalidates exactly executions `e1` and `e2`
- **AND** returns the invalidation response as JSON

#### Scenario: Cache invalidation rejects non-execution selectors
- **WHEN** a user runs `dml cache invalidate ck1`
- **THEN** the command fails because cache invalidation accepts execution refs only

### Requirement: Generated CLI SHALL expose cache lookup directly
The generated CLI SHALL expose `dml cache get <cache-key>` and `dml cache describe <cache-key>` from the shared cache namespace. Cache get SHALL serialize a reusable cached DAG ref as its canonical ref string or emit the established absent result. Cache describe SHALL serialize its structured cache description as JSON or emit the established absent result.

#### Scenario: Cache commands are generated
- **WHEN** a user inspects `dml cache --help`
- **THEN** `get`, `describe`, and `invalidate` appear as cache commands

#### Scenario: Cache get returns a ref
- **WHEN** a user runs `dml cache get ck1` and a cached DAG exists
- **THEN** the command prints the cached DAG ref

#### Scenario: Cache describe returns identities
- **WHEN** a user runs `dml cache describe ck1` and it names terminal execution `e1` with result `dag:d1`
- **THEN** the command returns JSON containing `execution = "index:e1"`, `dag = "dag:d1"`, and the execution lifecycle

### Requirement: Generated CLI SHALL expose one source-selectable GC command
The generated CLI SHALL expose `dml gc [--remote]` from `Dml.gc`. Without `--remote` it SHALL run local GC and serialize `LocalGCSummary`; with `--remote` it SHALL run configured remote GC and serialize `RemoteGCSummary`. It SHALL expose no dependency or dry-run option.

#### Scenario: Default GC command is local
- **WHEN** a user runs `dml gc`
- **THEN** local GC runs and its summary is printed as JSON

#### Scenario: Remote GC uses flag
- **WHEN** a user runs `dml gc --remote`
- **THEN** remote GC runs against configured `remote.root` and its summary is printed as JSON

#### Scenario: GC help omits unsupported selectors
- **WHEN** a user inspects `dml gc --help`
- **THEN** help exposes `--remote`
- **AND** it does not expose `--dep` or `--dry-run`

### Requirement: Admin exports the bundled agent skill
The generated CLI SHALL expose `dml skills querying`, `dml skills authoring`, `dml skills repository`, and `dml skills extensions`. Each command SHALL write its complete corresponding bundled skill document to standard output and SHALL not write command framing or serialized representation around that document. The CLI SHALL NOT expose `dml skills inspection`, `dml admin agent-skill`, or an `admin` namespace.

#### Scenario: User redirects a focused skill to a file
- **WHEN** a user runs `dml skills authoring > SKILL.md`
- **THEN** `SKILL.md` contains the complete bundled `authoring` skill document
- **AND** it begins with the skill document's YAML frontmatter

#### Scenario: User exports each focused skill
- **WHEN** a user runs each of `dml skills querying`, `dml skills authoring`, `dml skills repository`, and `dml skills extensions`
- **THEN** each command succeeds and prints only its corresponding bundled document

#### Scenario: Replaced inspection route is rejected
- **WHEN** a user runs `dml skills inspection`
- **THEN** command parsing fails because `inspection` is not a generated skill command

#### Scenario: Removed administrative route is rejected
- **WHEN** a user runs `dml admin agent-skill`
- **THEN** command parsing fails because `admin` is not a generated CLI namespace
