## 1. Runtime execution identity and storage

- [x] 1.1 Add runtime helpers for `active/<cache_key>` plain-text pointers and immutable `exec/<execution_id>.json` records under the remote S3 root.
- [x] 1.2 Update stale-lock handling so `start_fn` recovers an existing active execution when `exec/<execution_id>.json` exists and discards stale active pointers when it does not.
- [x] 1.3 Update the new-execution path to create a new `execution_id`, create the immutable execution record on the first `running` result, and create the active pointer only for non-terminal executions.

## 2. Adapter contract and execution flow

- [x] 2.1 Update adapter payload validation and invocation so envelopes include `execution_id` and `state` on both first launch and resume calls.
- [x] 2.2 Remove `pending` from adapter output validation and enforce `running|succeeded|failed` with the required `state`, `dag_id`, and `error` fields.
- [x] 2.3 Update `start_fn` result handling so resumed executions always use the immutable stored state and ignore replacement state returned by later adapter calls.
- [x] 2.4 Update terminal handling so adapter `failed` completes the DAG with the error and publishes the failed result to cache before surfacing the failure.

## 3. Executor and adapter migration

- [x] 3.1 Update contrib executors and adapters to return all durable resume state on the first `running` launch result and stop relying on executor-owned mutable resumable-state objects.
- [x] 3.2 Remove or replace executor-private state-prefix usage that conflicts with the runtime-owned execution-record model.
- [x] 3.3 Add executor-level coverage proving first-launch state is sufficient for later polling and that later returned replacement state is ignored.

## 4. Call-edge lineage indexes

- [x] 4.1 Add runtime helpers for `calls/from/index/<index_id>.json`, `calls/from/cache/<caller_ck>.json`, and `calls/to/cache/<callee_ck>.json` objects.
- [x] 4.2 Record call edges only on the new-execution path, after lock acquisition and inactive confirmation, using `index_id` for user-dag callers and `caller cache_key` for fn-dag callers.
- [x] 4.3 Implement read/merge/dedup/sort/conditional-write retry logic for all call-edge index updates.
- [x] 4.4 Add tests covering bidirectional lookup, deduped/sorted storage, concurrent update retries, and persistence of lineage across success and failure.

## 5. Documentation and regression coverage

- [x] 5.1 Update execution-model and adapter/runtime contract docs for `execution_id`, immutable execution records, active pointers, stale-lock recovery, and failed-result cache publication.
- [x] 5.2 Add or update docs for call-edge lineage storage, including the distinction between user-dags (no `argv`, no cache key) and fn-dags (`argv`, cache key).
- [x] 5.3 Add integration coverage for end-to-end new execution, resume, stale-lock recovery, failed-result caching, and call-edge recording.
