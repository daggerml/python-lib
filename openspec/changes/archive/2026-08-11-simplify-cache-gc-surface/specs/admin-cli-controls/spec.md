## MODIFIED Requirements

### Requirement: Administrative CLI flows are grouped under `dml admin`
Low-frequency recovery and agent-support commands SHALL remain under `dml admin`. Cache control and local or remote garbage collection SHALL be generated as top-level command surfaces rather than admin commands.

#### Scenario: Admin help contains only remaining administration commands
- **WHEN** a user inspects `dml admin` help
- **THEN** remaining index recovery and agent-support commands appear under `dml admin`
- **AND** cache, remote, and GC commands do not appear there

## ADDED Requirements

### Requirement: Cache invalidation accepts exact cache keys only
The generated CLI SHALL expose cache invalidation as `dml cache invalidate <cache-key> [more cache keys]`. It SHALL accept one or more exact cache keys and SHALL NOT accept DAG refs, argv refs, or other selector types.

#### Scenario: Cache invalidation accepts multiple exact keys
- **WHEN** a user runs `dml cache invalidate ck1 ck2`
- **THEN** the command invalidates those exact cache keys
- **AND** returns the existing invalidation response as JSON

#### Scenario: Cache invalidation rejects non-key selector forms
- **WHEN** a user runs `dml cache invalidate dag:abc123`
- **THEN** the command fails because cache invalidation accepts exact cache keys only

### Requirement: Generated CLI SHALL expose cache lookup directly
The generated CLI SHALL expose `dml cache get <cache-key>` from the shared cache namespace and SHALL serialize a cached DAG ref as its canonical ref string or emit the established absent result when no cache entry exists.

#### Scenario: Cache get command is generated
- **WHEN** a user inspects `dml cache --help`
- **THEN** `get` and `invalidate` appear as cache commands

#### Scenario: Cache get returns a ref
- **WHEN** a user runs `dml cache get ck1` and a cached DAG exists
- **THEN** the command prints the cached DAG ref

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

## REMOVED Requirements

### Requirement: Admin cache invalidation accepts exact cache keys only
**Reason**: Cache control moves from administration to the top-level generated cache namespace.

**Migration**: Replace `dml admin cache invalidate KEY...` with `dml cache invalidate KEY...`.

### Requirement: Admin remote list reports direct remote-root refs
**Reason**: Branch and tag endpoint inspection is now owned by `dml branch list --remote` and `dml tag list --remote`, including dependency endpoint selection and exact commit tips.

**Migration**: Replace `dml admin remote list` with separate `dml branch list --remote` and `dml tag list --remote` calls; add `--dep NAME` when inspecting a dependency endpoint.

### Requirement: Admin remote GC performs remote maintenance
**Reason**: Local and remote collection are consolidated into one top-level source-selectable GC command.

**Migration**: Replace `dml admin remote gc` with `dml gc --remote`.

### Requirement: Admin local GC supports dry-run inspection
**Reason**: Local collection moves to the top-level GC command, and the new shared signature intentionally supports only source selection rather than the previously specified dry-run option.

**Migration**: Replace `dml admin gc` with `dml gc`. There is no replacement for `dml admin gc --dry-run` in this change.
