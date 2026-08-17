## 1. S3 CAS Foundation

- [x] 1.1 Extend S3 CAS reads to return body, ETag, `LastModified`, and HTTP `Date`, with contract tests for timestamp parsing and conditional create/update/delete.
- [x] 1.2 Define the unified execution-record typed schema and validation for lock, adapter state, syntactically typed refs without storage existence checks, lineage, lifecycle, cancelation, and invalidation fields.
- [x] 1.3 Implement execution-record create, read, owner-checked CAS mutation, lock acquisition/steal, and owner-checked unlock using `LastModified + ttl <= Date`.
- [x] 1.4 Add parallel contract tests proving one lock winner, expired-lock stealing, unchanged expired-owner acceptance, stale-response rejection, and stale-unlock safety.

## 2. Cache And Execution Coordination

- [x] 2.1 Replace typed cache and active refs with plain `cache/<cache_key>` execution-ID CAS operations.
- [x] 2.2 Implement create-execution-before-cache-claim, conditional losing-record cleanup, winner reread, dangling-pointer recovery, and UUID7 attempt identity.
- [x] 2.3 Rewrite `get_or_start_fn` to resolve the current execution, acquire its embedded lock, preserve one execution across expiry, clean fresh artifacts on pre-adapter failure, and release ownership on every exit path.
- [x] 2.4 Add parallel tests for first-claim races, cleanup of losing attempts, running-execution reuse, stale pointer recovery, and cache-pointer persistence through terminal completion.

## 3. Adapter And Result Flow

- [x] 3.1 Rename persisted and envelope resume state to `adapter_state`, require object state only when fresh running work needs continuation, allow optional cancel state, and update all adapters/executors and protocol tests.
- [x] 3.2 Implement `running` as retry, `succeeded` as success, reported non-success outcomes as committed error DAGs with cached `result_ref`, and deliberate errors for unrecoverable malformed responses.
- [x] 3.3 Replace transport publication and reads with locked `result_ref` publication in the execution record; update worker activation and commit paths to acquire record ownership.
- [x] 3.4 Add tests proving adapter state is saved on every outcome, repeated execution-ID checks are idempotent, stale owners discard responses, and successful/error DAGs remain cache hits.

## 4. Lineage And Administration

- [x] 4.1 Update spawned/completed child registration to lock and CAS the caller execution record while retaining separate canonical caller-edge objects.
- [x] 4.2 Replace active-to-cancel-target moves with locked cancelation state, stored `argv_ref`, and conditional current-cache-pointer deletion.
- [x] 4.3 Store invalidation metadata in locked execution records, remove separate invalidation objects, and conditionally delete only matching cache pointers before marking each selected execution after reverse-edge planning.
- [x] 4.4 Add cancellation, invalidation, shared-child, registration rollback, rebound-pointer, and interrupted-pointer-deletion contract tests.

## 5. Remote Storage And GC

- [x] 5.1 Remove active, transport, cancel-target, launch, lock-file, and invalidation-object storage paths and their obsolete helpers.
- [x] 5.2 Update remote materialization and GC to trace execution-record `argv_ref` and `result_ref` roots and collect unreachable losing-attempt records.
- [x] 5.3 Replace old-layout tests with unified-layout integration tests against S3-compatible conditional-write behavior.

## 6. Public Surfaces And Documentation

- [x] 6.1 Preserve runtime inspection, cache lookup, graph, cancelation, and invalidation public behavior while adapting raw record and adapter-state reads to the unified schema.
- [x] 6.2 Update execution/cache, runtime-state, adapter/executor, remote/GC, glossary, and architecture documentation for the incompatible v0 layout.
- [x] 6.3 Run focused execution contracts, remote integration tests, full pytest, and Ruff; resolve all failures without adding legacy compatibility paths.

## 7. Contrib Protocol Follow-up

- [x] 7.1 Update Docker and Batch nested invoke payloads to use `adapter_state`.
- [x] 7.2 Preserve Batch job and nested adapter state on every poll and cancel response.
- [x] 7.3 Remove the CloudFormation executor, helper, plugin registration, specification, and documentation surfaces.
- [x] 7.4 Add focused nested-protocol tests and run full verification.

## 8. Contrib Schema Hardening

- [x] 8.1 Align Script worker/supervisor terminal schemas while preserving outer adapter state and cleanup.
- [x] 8.2 Forward explicit SSH operations and preserve Script/Docker cancel adapter state.
- [x] 8.3 Reject unknown operations, malformed adapter state, and cancel requests without `argv_ref`.
- [x] 8.4 Return object adapter state from Lambda exception envelopes.
- [x] 8.5 Add schema contract coverage and run full verification.
