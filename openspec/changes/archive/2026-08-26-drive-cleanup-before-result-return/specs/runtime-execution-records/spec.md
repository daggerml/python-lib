## MODIFIED Requirements

### Requirement: Cache refs SHALL remain proper refs and record execution ids
The runtime SHALL retain plain `cache/<cache_key>` execution-ID pointers. A cache reader SHALL resolve `state.json` and materialize its typed `result_ref`. Lifecycles `succeeded` and `failed` SHALL be reusable cache hits only when `result_ref` is present and neither cancelation nor invalidation blocks reuse. Cleanup SHALL be required when a result is present and `driver.cleanup` is null; `complete` and `failed` cleanup records SHALL be terminal and SHALL NOT require another cleanup call. A reusable result SHALL remain returnable while cleanup is pending, retry-delayed, complete, or failed. Before returning either a cached terminal result or a result established by a terminal invoke outcome, the coordinating caller SHALL give required cleanup one adapter call when it owns the driver and `driver.not_before` does not defer the operation. Cleanup retry SHALL persist continuation and timing while leaving cleanup required, and cleanup success or failure SHALL persist its terminal cleanup record. Cleanup retry, deferral, or failure SHALL NOT invalidate or replace the result.

#### Scenario: Successful terminal cache lookup
- **WHEN** `cache/ck1` contains `e1` and state is succeeded with `result_ref = "dag:d1"`
- **THEN** cache lookup materializes `dag:d1`

#### Scenario: Error DAG remains cached
- **WHEN** state is failed with an adapter-error DAG in `result_ref`
- **THEN** cache lookup returns that error DAG

#### Scenario: Cleanup does not block a reusable result
- **WHEN** a terminal execution has a reusable result and required cleanup is retry-delayed
- **THEN** the caller returns the result without calling cleanup before the shared deadline

#### Scenario: Running execution is not a cache result
- **WHEN** a cache pointer names a running execution
- **THEN** cache lookup reports that the result is not ready

#### Scenario: Cached terminal result drives required cleanup
- **WHEN** a caller owns a cached terminal execution with a result, null cleanup, and no active retry delay
- **THEN** it calls cleanup once and persists the cleanup response before returning the result

#### Scenario: Fresh terminal invoke drives required cleanup
- **WHEN** invoke establishes a successful or failed terminal result while its caller owns the driver and cleanup is null
- **THEN** that caller calls cleanup once and persists the cleanup response before returning the result

#### Scenario: Terminal cleanup is not repeated
- **WHEN** a reusable execution records cleanup as complete or failed
- **THEN** the caller returns the result without another cleanup call

#### Scenario: Cleanup retry preserves result delivery
- **WHEN** cleanup returns retry while a caller is preparing to return a reusable result
- **THEN** the caller persists adapter state and the shared retry deadline
- **AND** it returns the unchanged result with cleanup still required
