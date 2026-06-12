## Context

`src/daggerml/_core/db.pyx` currently mixes two concerns: a persistent DB-handle wrapper around the C LMDB layer and optional raw payload read/write modes on transaction methods. The raw modes are no longer used by live runtime code, while fork handling intentionally surfaces `DmlDbForkedError` and requires callers to reopen a new DB facade manually.

The requested behavior shifts responsibility downward: handle-level fork invalidation should be recovered inside `DmlDb`, while transaction objects that already crossed a process boundary should remain invalid. This keeps the retry boundary narrow and makes normal `DmlDB` usage fork-transparent.

## Goals / Non-Goals

**Goals:**
- Remove unused raw `get`/`put` behavior from `db.pyx` and typed wrappers that expose it.
- Make handle-level DB operations reopen and retry once when the C layer reports fork invalidation.
- Preserve the existing typed facade shape so higher-level callers keep using `DmlDB.tx()` and related helpers unchanged.
- Cover child-process behavior with tests that use the same logical DB object across a fork.

**Non-Goals:**
- Recover inherited transaction objects after a fork.
- Redesign the C API around a general reopen flag or multi-attempt retry protocol.
- Change typed serialization or object identity behavior.

## Decisions

### Decision: Recover only at the DB-handle boundary
`DmlDb` will own a private reopen helper that closes and recreates `self._handle` using the existing stored path, namespaces, and size settings. Handle-level methods such as `get_size()`, `resize()`, and transaction open will route through a small retry helper that detects fork-related return codes, rebuilds the handle, and retries once.

Why this approach:
- It keeps fork handling at the same layer that owns the persistent handle.
- It makes both direct raw-handle usage and the typed `DmlDB` facade benefit automatically.
- It avoids broad policy in `types.py` where some handle-level entry points could still be missed.

Alternative considered:
- Catch `DmlDbForkedError` only in `DmlDB.tx()`.
- Rejected because `DmlDB.call_with_resize()` and any future direct `DmlDb` handle methods would still leak fork recovery concerns.

### Decision: Keep transaction fork failure explicit
Only operations that start from a `DmlDb` handle will auto-recover. If a `DmlDbTxn` object itself is inherited across a fork, its methods will continue to fail with transaction-level fork errors.

Why this approach:
- A transaction snapshot belongs to the original process/thread.
- Replacing a DB handle cannot make an inherited transaction pointer valid again.

Alternative considered:
- Auto-recreate a transaction after transaction-level fork errors.
- Rejected because the original operation may already have observed partial state or caller-managed sequencing.

### Decision: Remove raw payload access from the DB Python surface
`DmlDbTxn.get()` and `put()` will become typed-only methods, and `TxnWithValid.get_raw()` / `put_raw()` will be removed.

Why this approach:
- Live code no longer uses the raw payload path.
- Removing it simplifies `db.pyx` retry logic and narrows the internal contract.

Alternative considered:
- Leave raw support in `db.pyx` but remove only the typed wrappers.
- Rejected because the user goal is to eliminate the debt entirely.

## Risks / Trade-offs

- Reopen-on-fork could hide unexpected process inheritance patterns -> Mitigation: retry only once and only for handle-level fork invalidation codes.
- Resize and transaction-open paths may need slightly different retry wiring than pure read helpers -> Mitigation: centralize the retry helper around C return-code inspection instead of duplicating ad hoc catches.
- Existing tests currently encode fail-fast child behavior -> Mitigation: replace them with scenarios that assert seamless child-process operations using the same DB facade, while preserving explicit failure tests for inherited transactions if needed.
