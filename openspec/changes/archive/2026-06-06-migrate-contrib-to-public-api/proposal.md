## Why

`daggerml.contrib` still depends on older core/runtime shapes, including private `_core` imports and `argv_ptr`-based worker DAG creation. The current public API already supports execution-aware worker DAG creation through `api.new(dml=..., cache_key=..., execution_id=...)`, so contrib should migrate to that surface without expanding or modifying core APIs.

## What Changes

- Migrate contrib DAG/session usage to `daggerml.api` or package-root public exports wherever existing public APIs are sufficient.
- Replace contrib worker DAG creation based on `argv_ptr` with `cache_key` plus `execution_id` worker DAG creation.
- Reconcile contrib adapter/executor parsing and result handling with the runtime envelope produced by the existing core implementation.
- Keep `temporary()` as repo/session creation only; callers must obtain worker DAGs from the temp `Dml` using `api.new(..., cache_key=..., execution_id=...)`.
- Remove stale contrib assumptions that adapter payloads must carry `argv_ptr`.
- Preserve existing contrib behavior for delayed authoring, adapters, executors, codecs, S3 helpers, status, and testing helpers except where the stale runtime protocol shape must be updated.
- **Implementation boundary:** application-code, test, and human-doc changes for this change are allowed only under `src/daggerml/contrib/**` and, if needed, existing contrib-specific tests/docs paths that are already scoped to contrib. No modifications are allowed to `src/daggerml/api.py`, `src/daggerml/_core/dml.py`, or any other non-contrib runtime/core/public API implementation file.

## Capabilities

### New Capabilities

- `contrib-public-api-migration`: Contrib modules use existing public DAG/session APIs and conform to the current runtime adapter envelope without requiring core or public API changes.

### Modified Capabilities

- `runtime-execution-records`: Clarify that this migration is constrained to contrib-owned protocol adaptation and must not require changes to runtime execution-record or public API implementations.

## Impact

- Affected implementation area: `src/daggerml/contrib/**` only.
- Forbidden implementation areas: `src/daggerml/api.py`, `src/daggerml/_core/**`, `src/daggerml/__init__.py`, non-contrib CLI/runtime/storage code, and unrelated tests/docs.
- Affected behavior: contrib adapters and executors should accept the existing runtime envelope, create worker DAGs from `cache_key` and `execution_id`, and continue returning canonical adapter results to the existing runtime caller.
- Dependencies: no new external dependencies are expected.
