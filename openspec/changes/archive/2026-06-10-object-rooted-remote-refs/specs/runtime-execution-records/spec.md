## MODIFIED Requirements

### Requirement: Cache refs SHALL remain proper refs and record execution ids
The runtime SHALL publish `refs/cache/<cache_key>.json` as a typed remote ref to the current DAG for that cache key, and that ref SHALL also record `execution_id` in `metadata`.

Readers that materialize cached results SHALL resolve the DAG through `ref.to`, and graph planners SHALL read `execution_id` from the same cache ref metadata.

#### Scenario: Successful execution updates cache pointer
- **WHEN** execution `e7` becomes the terminal cached result for cache key `ck1`
- **THEN** the runtime writes `refs/cache/ck1.json` with `ref.to = "dag:<oid>"`
- **AND** `metadata.execution_id = "e7"`

#### Scenario: Runnable DAG publication uses explicit execution identity
- **WHEN** an execution-aware worker commits a runnable DAG result
- **THEN** the runtime publishes the cache entry using the explicit `execution_id` provided through the runtime execution-aware call path
- **AND** it does not discover that identity through a process-local execution context object

### Requirement: Active execution refs SHALL point to argv roots
The runtime SHALL publish `refs/active/<cache_key>.json` as a typed remote ref to the `node-argv` root for the currently coordinated execution.

#### Scenario: Active execution stores argv root
- **WHEN** execution `e7` claims active coordination for cache key `ck1`
- **THEN** the runtime writes `refs/active/ck1.json` with `ref.to = "node-argv:<oid>"`
- **AND** `metadata.execution_id = "e7"`

#### Scenario: Terminal result does not change active root type
- **WHEN** execution `e7` later produces a terminal DAG result
- **THEN** the runtime publishes that DAG through `cache` or `transport`
- **AND** it does not overwrite `refs/active/ck1.json` with a `dag` root

### Requirement: Transport refs SHALL point to DAG roots
The runtime SHALL publish `refs/transport/<execution_id>.json` as a typed remote ref to a `dag` root.

#### Scenario: Finished execution publishes transport DAG
- **WHEN** execution `e7` finishes and publishes transport state
- **THEN** `refs/transport/e7.json` contains `ref.to = "dag:<oid>"`
- **AND** it contains integer `created` and object `metadata`
