## Context

DaggerML is still v0, but its current contract contains remnants of several completed development transitions. The DB layer exposes both `write_with_growth` and its old `call_with_resize` name; configuration readers silently discard unknown persisted keys; the native and Python DB surfaces retain unreachable handle-era errors and raw operations; executor cancellation translates wire-level `argv_ref` back to `argv_ptr`; and adapter response validation accepts arbitrary nonempty failure statuses. Storage has the same problem: initial format identifiers do not consistently start at zero, execution edges are implemented under `exec/edge/` despite the active `exec/edges/` contract, remote initialization checks only the `dml/` subtree, and remote GC can discover incomplete or unsupported execution layouts.

The active specifications and human documentation also preserve conflicting TOML, project-identity, migration-ledger, alias, and old protocol requirements. Keeping these alternatives makes it unclear which behavior is the initial product contract and causes tests to protect compatibility that has never shipped. Because these are v0 development formats and protocols, this change deliberately favors one strict contract over preserving development data or mixed-version interoperability.

### History-confirmed replacement manifest

Git history establishes the survivor for each actual replacement. Implementation SHALL keep the later canonical surface and delete the older surface listed here; archived change documents and vendored code are historical evidence, not maintained compatibility surface.

| Area | Older surface to delete | Later canonical surface to keep | Git evidence |
| --- | --- | --- | --- |
| DB growth | `call_with_resize` | `write_with_growth` | `7d0b1ea3` introduced the older helper; `b23ea6f9` (`feat: add resilient LMDB map growth`) introduced replayable repeated growth and retained the old name only as an alias. |
| DB payload API | Python `get(..., raw=...)` and `put(..., raw=...)` | typed `get(Ref)` and typed `put(value, ...)` | `77b0eb01` introduced raw flags; `7d0b1ea3` removed them from Cython runtime behavior but left stale stub declarations. |
| DB environment lifecycle | persistent-handle return codes and reopen behavior | process-local canonical-path environment registry plus current transaction errors | `77b0eb01` introduced handle-era errors; `de087f15` (`refactor: dedupe db env leases via registry`) removed the returning native paths and established registry acquisition. Directly raised `DmlDbInvalidTypeError` and reachable transaction errors remain current. |
| Configuration storage | TOML files, owner/project identity, `remote.project`, and named ordinary remotes | global/project `config.json` plus sole endpoint `remote.root` | `7d0b1ea3` introduced JSON config; `f75460e4` (`feat: simplify remote and dependency refs`) removed project identity and named remotes and made direct `remote.root` authoritative. |
| Codec ownership | `CodecContext` and staged internal normalization | codecs receiving the active `Dag`, with recursive normalization owned by `Dag` | `77b0eb01` introduced `CodecContext`; `68d169f6` removed it and established the Dag-owned contract; `7d0b1ea3` consolidated that contract in the current API. |
| Cancellation argument | executor keyword `argv_ptr` | `argv_ref` from execution metadata through wire and executor plugin | `872ec334` (`feat: unify remote execution state`) renamed the execution and wire field to `argv_ref` but left the executor translation bridge. |
| Adapter progress protocol | wire `status = "running"`, wire `state`, and old success/failure spellings | `retry`/`success`/diagnostic failure with wire `adapter_state` | `872ec334` established execution-owned `adapter_state`; `56d84c60` (`feat: split execution state and add cleanup`) replaced operation `running`/`succeeded`/`failed` with `retry`/`success`/failure. Execution lifecycle and backend status named `running`, plus executor-internal `state`, remain current. |
| Script source injection | `extra_lines` | `post_lines` | `7d0b1ea3` changed the accepted option and renderer to `post_lines`; later docs and skill text accidentally reintroduced the old spelling. |
| CLI maintenance routes | hand-written command modules and `admin remote`/`admin gc` maintenance routes | generated canonical commands, including `cache ...` and root `gc` | `30ba1920` introduced the generated CLI, `7d0b1ea3` consolidated it, and `e6bb8581` (`feat: simplify cache and gc surface`) removed maintenance routes without aliases. Canonical `admin agent-skill` remains. |
| Test organization | first-generation legacy/internal suites and active migration-ledger governance | subsystem-owned contract/integration taxonomy | `4078332c` migrated the first contract matrix; `7d0b1ea3` established current `tests/_core`, `tests/api`, and `tests/contrib` ownership. The archived ledger remains history; its active governance requirement is deleted. |
| Remote addressing | `refs/projects/<owner>/<project>/...` and URI-derived transport paths | direct one-project `refs/heads/...` and `refs/tags/...` at `remote.root` | `f75460e4` is the later clean replacement commit. |
| Execution records | active/cache/transport/cancel-target families and then unified `execution/<id>.json` | exact `metadata.json`, `state.json`, and `driver.json` split record plus plain cache pointer | `872ec334` replaced earlier ref families with unified records; `56d84c60` later replaced unified records with the exact split layout. |
| Call edges | cache-key `calls/from/...` and `calls/to/...`; accidental singular `exec/edge/...` implementation | execution-ID `exec/edges/<callee>/<caller>.json` | `730610ed` (`feat: align runtime execution state with execution-id graph controls`) explicitly specified the clean execution-ID replacement. `7d0b1ea3` later carried a singular-path inconsistency; plural `edges` remains the governing contract, so this is conformance repair rather than compatibility migration. |

Four cleanup classes are not history-confirmed old-to-later replacements and SHALL be represented honestly:

- The synthesized `meta` read alias has no later removal commit; persisted refs have always used canonical `metadata`. Removing `meta` is deletion-only API narrowing with no replacement adapter.
- Generic CLI alias machinery has no current caller. Its removal is dead-code deletion; canonical generated command names already exist.
- The unused `_core.types` aliases `MaybeRef`, `MaybeRefScalar`, `Collection`, `MaybeRefList`, `MaybeRefDict`, `MaybeRefCollection`, and `RefCollection` were introduced together by `77b0eb01`; Git does not establish a later alias family. All seven are deletion-only dead code, while the separately defined and used public `api.Collection` remains.
- Remote schema `2` and shallow version `1` are the chronologically later numeric IDs. Resetting both to integer `0` is an explicit v0 baseline renumbering, analogous to `664f1a9f` (`feat: add supervisor CloudWatch log streaming and reset protocol versions to 0`), not a claim that `0` was introduced later. The later layouts remain the one-project split-execution descriptor from `56d84c60` and shallow metadata shape from `6c54144e`; only their initial IDs are renumbered.

Deletion is complete only when `git grep` over maintained first-party source, tests, current docs, and active specs finds no older token or path except an explicit rejection assertion approved by the relevant spec. Whole obsolete files SHALL appear as deletions in `git diff --name-status`; symbols removed from retained files SHALL have zero maintained-tree matches and no import, stub, test, or documentation references.

The retired configuration/environment inventory is exact: `DML_DEFAULT_BRANCH`, `DML_PROJECT_NAME`, `DML_PROJECT_OWNER`, `DML_REMOTE_PROJECT`, `DML_REMOTE_NAME`, `DML_BRANCH`, `DML_REMOTE`, `DML_REMOTE_BUCKET`, `DML_REMOTE_PREFIX`, `DML_REPO`, `DML_DYNAMODB_TABLE`, `DML_REMOTE_CACHE`, and `DML_HOOK`. None may act as a configuration input, alias, or generated hook context. Current `DML_CONFIG_HOME`, `DML_DB_PATH`, `DML_DEFAULT_DB_MAP_SIZE_HEADROOM`, `DML_DEFAULT_DB_MAP_SIZE_MAX`, `DML_DEFAULT_BRANCH_NAME`, `DML_REMOTE_PRUNE_AGE_SECONDS`, `DML_PROJECT_HOME`, `DML_REMOTE_ROOT`, `DML_REMOTE_FETCH_WORKERS`, and `DML_USER` remain.

The CLI dead-code inventory is the unused `aliases` extraction in `_GroupedSubParsersAction.add_parser`, alias collision loop, alias registration loop, and `_GroupedChoicePseudoAction` alias parameter forwarding. Canonical name registration through `_name_parser_map` remains. Removed command references are `admin remote get-cache`, `admin remote invalidate-cache`, `admin gc`, and `admin remote gc`; canonical `cache get`, `cache invalidate`, root `gc`, `gc --remote`, and `admin agent-skill` remain.

The stale test/guidance inventory is also concrete: remove the alias-only DB test in `tests/_core/contracts/test_types.py`; replace the removed-config preservation case in `test_config_resolution.py` with strict rejection; replace old adapter payloads in `test_ssh_executor_contract.py`, `test_adapter_registry_contract.py`, and `test_executor_registry_contract.py`; remove obsolete `get_active`/`meta` shapes from `test_runtime_cancel_gates.py` and `tests/_core/helpers.py`; and update `test_agent_skill_contracts.py`, `AGENTS.md`, `src/daggerml/SKILL.md`, `docs/sharp-bits-and-security.md`, `docs/use/reference/cli.md`, `docs/use/reference/python-authoring.md`, `docs/develop/architecture/dag-storage-and-types.md`, and `c/README.md`. `CONTRIBUTING.md` SHALL no longer permit absent legacy test folders as an ongoing migration state. Negative tests that assert old inputs are rejected remain only where a current delta explicitly requires that rejection.

## Goals / Non-Goals

**Goals:**

- Land one coordinated hard replacement across Python, Cython, C, executor plugins, storage, configuration, CLI internals, tests, documentation, packaged guidance, and active OpenSpec specifications.
- Make every initial persisted format identifier the non-boolean integer `0` and reject all other values.
- Accept only the documented JSON configuration model and reject unknown, removed, or structurally invalid persisted keys.
- Make `write_with_growth` the sole growth-aware DB operation and remove unsupported raw and handle-era DB/C API surface.
- Preserve `argv_ref` unchanged from adapter wire requests through executor cancellation methods and nested transports.
- Define exact adapter request and response schemas and reject retired or contradictory status/error combinations.
- Define one exact remote endpoint layout, exact three-file execution records, and the canonical `exec/edges/` namespace.
- Remove stale tests, docs, specs, CLI alias machinery, type aliases, fixtures, and migration-governance clauses rather than restating them as deprecated behavior.
- Verify all affected layers together so code, generated/type surfaces, tests, docs, and specifications describe the same v0 contract.

**Non-Goals:**

- Migrating repositories, remotes, config files, execution records, plugins, or calls created with earlier v0-development shapes.
- Dual reads, dual writes, aliases, deprecation warnings, fallback parsing, or a compatibility window.
- Changing graph semantics, cache identity, execution lifecycle transitions, CAS encoding, or the split execution concurrency model beyond strict shape and path enforcement.
- Adding a new configuration source, DB capability, adapter operation, remote object family, or user-facing CLI command.

## Decisions

### Replace all compatibility surfaces in one shot

The implementation will remove old producers and consumers in the same change. There will be no intermediate release in which both names, paths, schemas, config models, or executor signatures work. Tests and specifications will assert the sole replacement contracts rather than retaining fixtures that demonstrate rejection through a migration path.

This intentionally overrides the repository's general phased migration guidance: these inputs and persisted shapes are v0 development artifacts, not released formats with compatibility obligations. A phased rollout would manufacture an obligation this cleanup exists to remove and would leave ambiguity about which contract is authoritative.

Alternative considered: deprecate aliases and support old and new records for one release. Rejected because it preserves every branch and fixture, permits mixed remote state, and provides no user benefit for an unreleased v0 format.

### Start initial format identifiers at integer zero

The one-project remote descriptor will be exactly:

```json
{
  "schema": 0,
  "hash": "sha256",
  "layout": "one-project-cas+refs+split-execution",
  "refs_prefix": "refs",
  "io_prefix": "io",
  "cas_prefix": "cas/sha256",
  "execution_prefix": "../exec"
}
```

Local `.dml/shallow.json` will be exactly `{"version":0,"missing":[...]}`, where `missing` is a sorted, unique array of exact `commit:<64 lowercase hex>` refs. Both validators will require a Python/JSON integer that is not a boolean and equals `0`; every other version, extra or missing field, wrong field type, or malformed ref will fail closed. Other initial versioned persisted payloads touched by this cleanup will follow the same non-boolean integer-zero rule.

Alternative considered: retain existing arbitrary version numbers because changing them invalidates development data. Rejected because this is the point at which the project establishes its first coherent format numbering and no migration is promised.

### Validate persisted JSON configuration exactly

Global `config.json` and project `.dml/config.json` remain partial override documents, but every object level will be validated before flattening or precedence resolution. Leaves may be omitted, but accepted leaves are limited to the canonical resolver keys: `config_home`, `db_path`, `project_home`, `user`, `default.{db_map_size_headroom,db_map_size_max,branch_name}`, and `remote.{root,fetch_workers,prune_age_seconds}`. Persisted project identity, named-remotes, branch selection, hooks from removed models, TOML-era fields, obsolete execution/cache fields, and every unknown key will raise a descriptive configuration error. Invalid root JSON types and a scalar where an accepted nested object is required will also fail rather than being ignored or rewritten.

Updates will validate the complete existing document before changing it and will write only supported JSON keys. Resolution precedence remains explicit input, environment, project JSON, global JSON, then defaults, but only currently mapped environment variables participate; removed variables are not aliases and are not preserved as contract documentation.

Alternative considered: continue selecting known flattened keys and silently preserve the rest. Rejected because typos and obsolete fields then appear effective, and a later update perpetuates an invalid file.

### Narrow the DB and C boundaries to implemented operations

`DmlDb.write_with_growth(fn, create_if_missing=False)` and typed `DmlDB.write_with_growth(...)` will be the only growth-aware write entry points. Both `call_with_resize` methods, their stub declarations, and alias-specific tests will be removed. The Python stub will expose only typed transaction `get` and `put` signatures; raw flags or raw helper declarations will not be part of the supported Python boundary.

The dead C return codes `DML_DB_ERR_HANDLE_INVALID`, `DML_DB_ERR_HANDLE_CLOSED`, `DML_DB_ERR_HANDLE_FORKED`, `DML_DB_ERR_TYPE_INVALID`, and `DML_DB_ERR_ENV_REOPENED`, corresponding Cython declarations and unreachable return-code mappings will be removed because no current native path returns them. Python exception classes SHALL be removed only when Git and current callers confirm they exist solely for those mappings. In particular, directly raised `DmlDbInvalidTypeError` and reachable transaction invalid/read-only/fork errors remain. Numeric C error values may be compacted because v0 does not promise a stable external C ABI.

Alternative considered: retain the names in stubs and headers as harmless conveniences. Rejected because declarations imply supported behavior, expand downstream branching, and conceal which errors the implementation can actually produce.

### Use `argv_ref` end to end and validate operation schemas exactly

The cancel request field and executor keyword will both be named `argv_ref`. `ExecutorBase.cancel`, every built-in executor, nested Docker/SSH/Batch/script forwarding, plugin guidance, fixtures, and tests will use that name directly; the dispatcher will not translate to `argv_ptr`.

Requests will reject unspecified fields and use these exact schemas:

```text
invoke  = operation, cache_key, execution_id, remote, runnable,
          adapter_state, scratch_uri
cleanup = invoke fields + result_ref
cancel  = invoke fields + argv_ref, requested_by
```

`operation` must be the matching literal; `cache_key`, `execution_id`, and `scratch_uri` must be nonempty strings; `remote` must be exactly `{"root": <nonempty string>}`; `runnable` must be an object; and `adapter_state` must be an object or null. Cleanup additionally requires a syntactically valid `dag` ref. Cancel additionally requires a syntactically valid `node-argv` ref and string-or-null `requested_by`.

Responses will be objects containing only `status`, optional `adapter_state`, optional `retry_after_ms`, and optional `error`. `adapter_state` is object-or-null, and `retry_after_ms` is a non-boolean nonnegative integer allowed only with `retry`. The accepted combinations are exact:

- Invoke and cleanup `success`: `error` is absent or null; retry delay is absent; adapter state may be object or null.
- Invoke and cleanup `retry`: object `adapter_state` is required; `error` is absent or null; optional retry delay is valid.
- Invoke and cleanup failure: `status` is a nonempty code other than `success`, `retry`, `running`, or `cancelled`; nonempty `error` is required; retry delay is absent.
- Cancel `cancelled`: `error` is absent or null; retry delay is absent; adapter state may be object or null.
- Cancel `retry`: the same retry shape as above.
- Cancel failure: `status` is a nonempty code other than `cancelled`, `retry`, `running`, or `success`; nonempty `error` is required; retry delay is absent.

The retired `running` status is always malformed rather than a synonym for `retry`. Success with error text, failure without error text, retry without state, retry-only fields on terminal outcomes, booleans used as integers, and operation-specific success statuses used by the wrong operation are protocol errors. Existing runtime consequences remain: invoke failures publish adapter-error DAGs, cleanup failures record diagnostics without changing the result, and unsuccessful cancellation remains eligible for bounded retry.

Alternative considered: normalize `running` to `retry` and continue treating arbitrary nonempty statuses as failure. Rejected because it accepts ambiguous plugins and contradictory payloads at the boundary where errors can be reported precisely.

### Make the endpoint and execution layouts singular

Resolved `remote.root` is one project and one execution domain. Its complete supported key layout is:

```text
dml/dml.json
dml/refs/heads/<quoted-name>.json
dml/refs/tags/<quoted-name>.json
dml/refs/tombstone/...
dml/cas/sha256/<aa>/<bb>/<oid>
exec/cache/<cache_key>
exec/execution/<execution_id>/metadata.json
exec/execution/<execution_id>/state.json
exec/execution/<execution_id>/driver.json
exec/edges/<callee_execution_id>/<caller_execution_id>.json
exec/io/<execution_id>/...
```

Initialization will test emptiness across the entire endpoint root, not merely `remote.root/dml/`. If `dml/dml.json` is absent and any key exists anywhere under the endpoint, including `exec/`, initialization fails without mutation. A truly empty endpoint conditionally creates the exact v0 descriptor before any refs, CAS, cache, execution, edge, or IO state. Read-only inspection may use one bounded whole-endpoint existence probe but never initializes. A present descriptor must equal the exact object above; missing, extra, differently typed, boolean, or nonzero fields are unsupported.

Each execution consists of exactly the following three JSON files and no unified `exec/execution/<id>` object:

```json
{"execution_id":"string","cache_key":"string|null","argv_ref":"node-argv ref|null","created_at":"nonnegative integer"}
```

```json
{"lifecycle":"pending|running|succeeded|failed|cancel-pending|canceled","result_ref":"dag ref|null","result_source":"runtime|adapter-error|null","spawned_execution_ids":["string"],"child_execution_ids":["string"],"cancelation":{"requested_by":"string","requested_at":"nonnegative integer"},"invalidation":{"requested_by":"string","requested_at":"nonnegative integer"},"updated_at":"nonnegative integer"}
```

```json
{"lock":{"owner":"string","ttl":"positive finite number"},"not_before":"nonnegative integer|null","adapter_state":"object|null","cleanup":{"status":"complete|failed","error":"string|null"}}
```

The shown `cancelation`, `invalidation`, `lock`, and `cleanup` objects are nullable. Existing cross-field invariants remain mandatory: result ref and source are both null or both valid; lineage arrays contain unique nonempty disjoint IDs; completed cleanup has null error and failed cleanup has diagnostic text; timestamps reject booleans; and all objects reject extra or missing fields.

Remote GC will enumerate execution IDs from the three-file namespace, require exactly one valid metadata, state, and driver object for each discovered ID, and fail closed on partial records, extra files in an execution directory, malformed files, or unified execution objects. It will derive retention only from validated split records, cache pointers, lineage/control policy, and lock state, and will trace CAS liveness only from `metadata.argv_ref` and `state.result_ref`. It will not tolerate, parse, preserve specially, or migrate unsupported execution shapes.

Alternative considered: let initialization ignore the sibling execution prefix and let GC skip unknown records. Rejected because both choices allow incompatible state to coexist under an endpoint that claims one exact descriptor.

### Use only the canonical execution-edge path

All producers, readers, cancellation/invalidation traversal, cleanup, GC-related lineage logic, tests, docs, and specifications will use `exec/edges/<callee_execution_id>/<caller_execution_id>.json`. The payload is exactly:

```json
{"caller_execution_id":"string","callee_execution_id":"string"}
```

The singular `exec/edge/` implementation path and any older call-edge namespace are removed without fallback reads or duplicate writes.

Alternative considered: read both singular and plural paths while writing the plural path. Rejected because it creates a permanent merge problem for caller liveness and makes cancellation depend on hidden compatibility state.

### Delete stale contract artifacts instead of preserving historical aliases

The implementation will remove the enumerated migration-only and alias-specific tests/fixtures, stale guidance, CLI alias branches, and seven unused `_core.types` aliases. Human docs will describe JSON config, direct one-project endpoints, Dag-owned codec normalization, `post_lines`, `argv_ref`, strict statuses, and the exact v0 storage layouts. Active specs will remove completed migration wording, TOML/project-identity/named-remote conflicts, old contrib scope restrictions, unified execution references, and completed test-migration requirements. Historical archived changes remain historical unless repository policy requires correcting an active link; they do not define current behavior.

Alternative considered: leave stale docs and aliases because runtime behavior would still be correct. Rejected because this change's purpose is a singular contract, and users and maintainers consume these surfaces as part of that contract.

## Risks / Trade-offs

- [Old v0 repositories, remotes, shallow files, config files, and plugins fail immediately] -> Fail with specific validation errors and require recreation or source/plugin updates; do not obscure incompatibility with fallback behavior.
- [A cross-layer partial implementation creates an unusable mixed protocol] -> Land source, native declarations, stubs, built-ins, tests, docs, and active specs as one coordinated change and verify the complete diff before release.
- [Strict config validation exposes previously ignored typos] -> Report the file and unsupported key or invalid shape so the user can correct the JSON directly.
- [Whole-root initialization treats unrelated objects under the endpoint as incompatible] -> This is intentional because `remote.root` owns the complete project and execution domain; users must choose an empty dedicated prefix.
- [Strict GC stops on one malformed execution instead of collecting other objects] -> Prefer fail-closed liveness over deleting CAS based on incomplete roots; repair means clearing the v0 development endpoint and recreating state.
- [Removing C error values and Python names breaks internal consumers] -> Update every in-repository declaration, mapping, stub, and test together; no external ABI compatibility is promised before v1.
- [Exact status validation rejects loosely implemented executors] -> Update all first-party executors and protocol tests together and make malformed combinations produce deliberate boundary errors.
- [Documentation and active specs drift during broad cleanup] -> Search for every removed name, path, format, config model, and migration phrase, then include zero-match assertions or focused checks in coordinated verification.

## Migration Plan

1. Replace version constants, strict validators, descriptor initialization, execution/edge paths, and GC shape handling as one storage-format update.
2. Remove DB aliases, raw stub operations, dead C/Cython errors and mappings, and update the typed/native contract tests in the same change.
3. Rename executor cancellation parameters to `argv_ref`, tighten request/response validation, and update all built-in and nested executors together.
4. Enforce exact JSON config keys and remove project-identity, TOML, obsolete environment, CLI alias, source-injection, and type-alias remnants.
5. Replace compatibility-oriented tests and fixtures, then update user docs, contributor docs, packaged guidance, and all affected active OpenSpec capabilities to the singular contracts.
6. Run focused DB/native, config, remote, execution, contrib, CLI, docs/spec, and type-checking tests before the complete lint and test suites. Search the repository for removed symbols and paths, and exercise empty/non-empty endpoint initialization plus malformed and exact three-file GC records.

There is no data migration, dual path, compatibility alias, staged rollout, or deprecation period because all affected formats are v0. Development repositories, endpoints, shallow metadata, and persisted config using removed shapes must be recreated or manually rewritten to the exact new contract; plugins must be updated before use.

Rollback consists only of reverting the coordinated code, tests, documentation, and specification change. Rollback testing must use fresh local and remote test data appropriate to the reverted code. New and old layouts are not mutually readable, and rollback does not convert, preserve, or recover data written by the replaced implementation.
