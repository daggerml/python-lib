## REMOVED Requirements

### Requirement: Cache invalidation accepts exact cache keys only
**Reason**: Cache keys are mutable bindings and cannot safely identify one execution attempt during invalidation traversal.

**Migration**: Resolve a cache key with `dml cache describe <cache-key>`, then pass its returned execution ref to `dml cache invalidate <execution-ref>`.

## ADDED Requirements

### Requirement: Cache invalidation accepts exact execution refs only
The generated CLI SHALL expose cache invalidation as `dml cache invalidate <execution-ref> [more execution refs]`. It SHALL accept one or more `index:` or `frozenindex:` refs and SHALL NOT accept cache keys, bare execution IDs, DAG refs, argv refs, or other selector types.

#### Scenario: Cache invalidation accepts multiple execution refs
- **WHEN** a user runs `dml cache invalidate index:e1 frozenindex:e2`
- **THEN** the command invalidates exactly executions `e1` and `e2`
- **AND** returns the invalidation response as JSON

#### Scenario: Cache invalidation rejects non-execution selectors
- **WHEN** a user runs `dml cache invalidate ck1`
- **THEN** the command fails because cache invalidation accepts execution refs only

## MODIFIED Requirements

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
