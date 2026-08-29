## Why

DaggerML is still v0, but the repository retains aliases, stale protocol names, removed configuration models, permissive legacy-shaped reads, and migration-era specifications as though released predecessors must remain supported. Removing that baggage now keeps the initial contract singular and strict before external compatibility obligations exist.

## What Changes

- **BREAKING** Remove the `call_with_resize` database aliases and expose `write_with_growth` as the only growth-aware write API.
- **BREAKING** Remove raw DB operations from the type stub and retire dead handle-era C errors and Python mappings that no implementation path returns.
- **BREAKING** Reject unknown and removed persisted configuration keys instead of silently ignoring and preserving them; remove remaining project-identity, TOML, and obsolete environment-variable contracts.
- **BREAKING** Keep cancellation argument naming as `argv_ref` across wire, adapter dispatch, executor plugins, built-in executors, nested execution, docs, and tests; remove the `argv_ptr` translation.
- **BREAKING** Reject the retired `running` adapter status and other malformed status/error combinations rather than treating every nonempty status as valid.
- Remove stale `extra_lines` guidance and contract fixtures; `post_lines` is the sole supported source-line injection option.
- Remove unused generic CLI alias machinery and obsolete migration-oriented public documentation.
- Start every initial persisted format identifier at the non-boolean integer `0`, including the remote descriptor and shallow-history metadata, while continuing to reject every other version.
- Use only the canonical `exec/edges/` call-edge namespace required by the active specification.
- Require remote initialization to establish emptiness across the entire endpoint root, including execution state, before writing the v0 descriptor.
- Make remote GC validate exact three-file v0 execution records and retire unsupported unified execution objects rather than tolerating or preserving legacy shapes.
- Remove stale compatibility-oriented type aliases, tests, fixtures, active spec clauses, and permanent migration-ledger requirements where they no longer describe the v0 product.
- Correct conflicting current documentation and active specifications so they describe only JSON configuration, direct one-project remotes, Dag-owned codec normalization, current plugin protocols, and the singular v0 storage layouts.
- Record the Git commit that established each later canonical replacement and require deletion of the older surface. Where history shows deletion-only cleanup or deliberate version-ID renumbering instead, label that distinction explicitly rather than inventing chronology.

## Capabilities

### New Capabilities

None.

### Modified Capabilities

- `shared-internal-configuration`: Require exact persisted configuration keys and remove project-identity compatibility contracts.
- `remote-project-refs`: Define the sole direct-endpoint JSON model, exact version-0 descriptor, and whole-root initialization check without legacy migration behavior.
- `git-like-commit-ops`: Replace the obsolete TOML recovery path with strict JSON configuration recovery.
- `db-write-with-growth`: Make `write_with_growth` the sole growth-aware write entry point.
- `db-handle-lifecycle`: Remove obsolete raw and handle-era compatibility requirements from the Python/C boundary.
- `db-env-registry`: Describe process and fork safety without exposing retired raw-handle recovery surfaces.
- `shallow-history-materialization`: Define the exact initial shallow metadata schema with version identifier `0`.
- `codec-normalization`: Collapse the completed two-stage migration into the sole current Dag-owned codec contract.
- `adapter-operation-protocol`: Preserve `argv_ref` through executor dispatch and reject retired or malformed response statuses.
- `contrib-public-api-migration`: Remove old `running` semantics and old/new cancellation argument translation from the contrib contract.
- `remote-object-refs`: Define liveness through exact split execution records and reject unsupported execution shapes during GC.
- `runtime-execution-records`: Replace remaining embedded/unified lock wording with exact `driver.json` ownership while retaining the later split-record lifecycle.
- `execution-admin-controls`: Assign invalidation locking and semantic tombstones to exact driver and state files instead of a unified record.
- `execution-call-edges`: Assign child-registration mutation and failure cleanup to exact split files while preserving canonical `exec/edges/` records.
- `test-contract-matrix`: Remove the completed migration ledger as a permanent governance requirement while retaining no-duplicate-test outcomes.

## Impact

The change affects the typed and Cython database surfaces, first-party C error definitions, configuration resolution, shallow metadata, remote descriptors and GC, execution call edges, contrib executor signatures and response validation, CLI internals, tests, packaged agent guidance, user and contributor documentation, and active OpenSpec specifications. The history-confirmed replacement manifest in `design.md` is normative for choosing each survivor. No compatibility adapters, data migration, dual reads, dual writes, aliases, or deprecation window will be added; repositories, remotes, plugins, and calls using removed v0-development shapes will fail clearly and must be recreated or updated.
