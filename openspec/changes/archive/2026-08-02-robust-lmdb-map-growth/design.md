## Context

The C database registry leases one LMDB environment per canonical path while transactions are active. Today, a map-full retry estimates a larger map size and opens a replacement transaction, but an already leased environment is reused and its map-size argument is intentionally ignored. The retry can therefore use the same full map. Most write paths also open transactions directly, so they do not participate in the existing one-shot recovery helper.

The existing collection model, including its 100,000-item ceiling, remains unchanged. A DAG near that ceiling can produce large immutable snapshots, so the database layer must handle a write whose space requirement is larger than the configured growth headroom.

## Goals / Non-Goals

**Goals:**
- Make replayable local write functions recover from map-full until they commit or the configured map-size maximum is reached.
- Make an explicit native resize request block new transactions for one canonical database path until active leases drain and the environment is reopened.
- Preserve normal transaction concurrency by keeping `map_size` an initial-open setting that is ignored for an already-open environment.
- Ensure a process can adopt map growth performed by another process before retrying a transaction open.
- Keep external side effects outside retried write functions.

**Non-Goals:**
- Change the 100,000-item collection limit, DAG representation, or immutable-object behavior.
- Guarantee success after disk, address-space, filesystem, or configured map-size capacity is exhausted.
- Serialize ordinary reads or writes when no resize is pending.
- Retry arbitrary user code or external effects.

## Decisions

### Use `write_with_growth(fn)` for replayable local writes

The typed DB facade will expose an internal write helper that runs `fn(txn)` in a write transaction. On map-full from either an operation or commit, it aborts the attempt, synchronously requests a larger map, and reruns `fn`. It repeats until commit succeeds or further growth is impossible.

`fn` is an implementation function whose work is limited to deterministic local reads, validation, and writes. Callers split remote coordination, adapter invocation, filesystem pointer updates, and other externally visible effects into phases outside `fn`.

Alternative considered: transparently retry every `with db.tx()` block. Rejected because a context manager cannot replay the caller's block after map-full, and retrying arbitrary code can duplicate side effects.

### Add an explicit blocking native resize operation

The C registry will track per-slot resize state and use a condition variable. A resize request marks its slot as resizing, rejects no existing leases, waits for all active leases to close, closes the old environment, and opens a replacement environment at the requested larger size. Transaction acquisition observes the state and waits until the resize gate clears, then performs normal environment acquisition. The resize requester receives the replacement-open result; a failed resize does not latch an error on the slot.

The resize operation is explicit rather than inferred from `dml_db_txn_open(..., map_size=...)`. A `map_size` passed to normal transaction open remains ignored when an environment is already open. This preserves concurrent normal mutation transactions and reserves the blocking drain behavior for recovery and explicit resize only.

Alternative considered: resize the live environment whenever transaction open requests a larger map. Rejected because incidental transaction options would serialize routine traffic and change established `map_size` semantics.

### Grow repeatedly by configured headroom within the configured limit

After each map-full attempt, native resize opens the backing environment, reads `MDB_envinfo.me_mapsize`, and increases it by the configured headroom, capped by `max_map_size`. It retries while the map can advance. If the map is already at the maximum, it returns a dedicated result which the typed layer turns into a terminal capacity error identifying the database path, current size, and configured maximum. Other resize failures retain their native error types.

Alternative considered: a single `database-size + headroom` retry. Rejected because one large immutable DAG write can exceed headroom and because a fixed increment causes repeated stalls for growth-heavy workloads.

### Recover map-size changes made by other processes

The native layer will recognize LMDB's map-resized result during transaction acquisition. It will use the same drain-and-reopen coordination to adopt the backing environment's current map size, then retry the transaction acquisition once. This is separate from local map-full growth: another process determined the new size.

### Migrate write sites by side-effect boundary

Core local graph mutations, commits, remote object materialization, adapter-error persistence, and GC will use `write_with_growth`. Each site will be reviewed so a retried `fn` neither invokes an adapter nor changes remote/file state. `start_fn` becomes local preparation, remote coordination, and local attachment phases; each local write phase uses the helper.

## Risks / Trade-offs

- [A long-lived transaction delays growth] → The resize gate waits rather than invalidating active readers or writers; tests cover wait-and-release behavior.
- [A write function is not replay-safe] → Limit `fn` to internal storage implementation functions and audit each migrated site for external effects.
- [Repeated resize attempts add latency] → Use a configured fixed headroom, wake waiters immediately after resize, and stop at a contextual terminal capacity error.
- [Cross-process resize invalidates a mapped environment] → Handle LMDB map-resized acquisition failures through the same explicit drain/reopen protocol.
- [Large immutable DAG snapshots still consume space] → Retain current collection and DAG contracts; users can GC unreachable history. Representation changes are out of scope.

## Migration Plan

1. Add native resize coordination and tests while preserving existing transaction-open behavior.
2. Add the typed growth helper and capacity diagnostics.
3. Migrate replayable write sites in bounded groups with map-full tests.
4. Run existing storage, runtime, remote, and full test suites.

No persisted-data migration is required. Rollback consists of reverting the library version; LMDB maps grown by a newer version remain valid databases for the prior version, subject to its configured maximum and operating-system mapping limits.

## Open Questions

- Does the fixed headroom provide an acceptable latency versus virtual-address trade-off across supported platforms?
- Should terminal capacity be represented by a dedicated DB exception class or a contextualized `DmlDbMapFullError`?
