---
status: specified
doc_type: spec
---

# S3 Store

## Authority

This document is authoritative for the interface and behavior of `daggerml.contrib.s3` utility surfaces:

- `is_s3_uri`,
- `S3Store` construction and root resolution,
- URI/name normalization,
- content-addressed object read/write/list/delete behavior,
- JSON helper behavior,
- archive creation and extraction behavior,
- prefix rebasing behavior.

This document is not authoritative for:

- `Uri` lifecycle semantics,
- runtime configuration key definitions,
- remote protocol-root layout under `<remote.root>/dml/`,
- contrib adapter/executor orchestration semantics,
- provider-specific S3 API semantics outside the interface behavior defined here.

If related docs conflict on items in scope, this document is the source of truth.

## Scope

This document defines:

- the accepted inputs and outputs of `daggerml.contrib.s3` utility interfaces,
- how default bucket/prefix resolution derives the S3Store Data Root,
- how names, `Uri` values, and `Node` values are normalized into bucket/key pairs,
- how `S3Store` performs content-addressed writes, reads, listing, deletion, JSON serialization, tar creation, tar extraction, and prefix rebasing,
- the safety boundary for archive extraction.

This document does not define:

- repository CAS, refs, or transport object layout,
- `remote.root` configuration ownership,
- external artifact GC or `Deletable` ownership,
- contrib runtime payload schemas that happen to reference S3-backed artifacts.

## Purpose

`S3Store` provides a stable contrib utility for storing external payloads in S3-backed object storage by content hash while keeping utility-path semantics distinct from repository remote protocol storage.

## Glossary

- Data Root: the default `S3Store` prefix derived from the configured Project Root as `<project-prefix>/data` or `data` when the Project Root has no path component.
- CAS: the content-addressed storage concept defined in [../remote-data-model.md](../remote-data-model.md).
- Deletable: defined in [../storing-and-retrieving-external-data.md](../storing-and-retrieving-external-data.md).
- IAM: identity and access management policy controls owned by the storage provider and outside this document's authority.
- DmlConfig: the runtime configuration resolver described in [../configuration.md](../configuration.md).
- DmlRepoError: the repository error type used by `daggerml.contrib.s3` for deterministic caller, configuration, and archive-safety failures.
- IAM: provider-side identity and access management policy controls; detailed IAM semantics are outside this document's authority.
- JSON: JavaScript Object Notation values serialized and parsed through Python `json.dumps(...)` and `json.loads(...)` as specified by this document.
- Name: a non-`s3://` string passed to `S3Store` that is resolved relative to the current `bucket` and `prefix`.
- Node: a value exposing `.value()` and accepted by `S3Store.parse_uri(...)` for normalization.
- POSIX Path Semantics: slash-delimited path normalization that collapses `.` and `..` segments using a synthetic root at `/`.
- Project Root: defined in [../configuration.md](../configuration.md) as the configured `remote.root` S3 location.
- Protocol Root: defined in [../remote-data-model.md](../remote-data-model.md) as the repository-managed namespace rooted at `<remote.root>/dml/`.
- S3 URI: a string of the form `s3://<bucket>/<key>`.
- S3Store: the dataclass utility in `daggerml.contrib.s3` that performs S3-backed external object operations.
- Unsafe Extraction: `S3Store.untar(..., unsafe=True)`, which disables the default path-containment validation.
- Uri: the external-resource datum defined by [../storing-and-retrieving-external-data.md](../storing-and-retrieving-external-data.md).

## Contract

### Interfaces

- General argument handling:
  - no interface in this document accepts extensible object payloads with optional unknown fields,
  - unspecified object fields are therefore not applicable,
  - extra Python positional or keyword arguments beyond the declared callable signature MUST be rejected by the callable boundary.
- `is_s3_uri(value: str) -> bool`:
  - returns `True` iff `value` parses as scheme `s3`, has a non-empty bucket component, and has a non-empty path other than `/`,
  - returns `False` for non-S3 schemes, missing bucket, or missing key path,
  - has no side effects.
- `S3Store(bucket: str | None = None, prefix: str | None = None, client: Any = None)`:
  - when both `bucket` and `prefix` are `None`, construction MUST resolve runtime config through `DmlConfig.resolve()` and use `remote.root` as the Project Root,
  - when resolved `remote.root` is absent, construction MUST fail with `DmlRepoError`,
  - when resolved `remote.root` is present but is not an `s3://` URI with a bucket, construction MUST fail with `DmlRepoError`,
  - when `remote.root` is `s3://<bucket>/<base>`, the default Data Root MUST be `<base>/data`,
  - when `remote.root` is `s3://<bucket>`, the default Data Root MUST be `data`,
  - when `bucket` is still `None` after default resolution, construction MUST fail with `DmlRepoError`,
  - when `prefix` is `None`, it MUST normalize to the empty string,
  - stored `prefix` MUST be normalized by stripping leading and trailing `/`,
  - when `client` is `None`, construction MUST create an S3 client via `_boto3_client("s3")`,
  - when boto3 import fails during default client creation, construction MUST fail with `DmlRepoError`,
  - failures raised by the provider client constructor after boto3 import succeeds MUST propagate,
  - construction has no remote S3 side effect beyond possible local client creation.
- `S3Store.from_remote_root(remote_root: str) -> S3Store`:
  - accepts only an `s3://` Project Root with a non-empty bucket,
  - derives the same Data Root rule as default construction,
  - returns a new `S3Store` with derived `bucket` and `prefix`,
  - invalid `remote_root` MUST raise `DmlRepoError`.
- `S3Store.parse_uri(name_or_uri) -> tuple[str, str]`:
  - accepts `str`, `Uri`, or `Node`,
  - for `Node`, MUST call `.value()` first,
  - for `Uri`, MUST normalize from `.uri`,
  - any other input type MUST raise `DmlRepoError`,
  - for an `s3://` string, MUST return `(bucket, key)` from the parsed URI without rebasing onto the current prefix,
  - for a Name, MUST return `(self.bucket, "<prefix>/<name>")` when `prefix` is non-empty and `(self.bucket, name)` when `prefix` is empty,
  - has no S3 side effects.
- `S3Store.put(data: bytes | None = None, filepath: str | None = None, *, suffix: str = "") -> Uri`:
  - exactly one of `data` or `filepath` MUST be provided,
  - when `filepath` is used, the method MUST read local file bytes before hashing,
  - the object name MUST be `sha256(payload-bytes) + suffix`,
  - the method MUST write those bytes by calling `put_object(Bucket=<bucket>, Key=<key>, Body=<bytes>)`,
  - the returned value MUST be `Uri("s3://<bucket>/<key>")`,
  - side effects are a local file read when `filepath` is used and a single S3 object write,
  - local file-read failures and S3 client failures MUST propagate.
- `S3Store.get(name_or_uri) -> bytes`:
  - MUST normalize the identifier through `parse_uri(...)`,
  - MUST read object bytes through `get_object(Bucket=<bucket>, Key=<key>)`,
  - MUST return the full response body bytes,
  - backend read failures MUST propagate.
- `S3Store.exists(name_or_uri) -> bool`:
  - MUST normalize the identifier through `parse_uri(...)`,
  - MUST call `head_object(Bucket=<bucket>, Key=<key>)`,
  - MUST return `True` when the head request succeeds,
  - MUST return `False` when the backend error code is exactly `404`, `NoSuchKey`, or `NotFound`,
  - any other backend failure MUST propagate unchanged.
- `S3Store.ls(s3_root=None, *, recursive: bool = False, lazy: bool = False) -> list[Uri] | Iterable[Uri]`:
  - when `s3_root` is omitted, listing MUST begin at the current `S3Store` root,
  - when `s3_root` is provided, it MUST accept the same identifier categories as `parse_uri(...)` and MUST normalize through `parse_uri(...)`,
  - when the resolved prefix is non-empty, listing MUST use that prefix with a trailing `/`,
  - when `recursive` is `False`, listing MUST call `list_objects_v2` pagination with `Delimiter="/"`,
  - when `recursive` is `True`, listing MUST omit `Delimiter`,
  - the interface MUST yield or return only object `Contents` entries; directory prefixes are not materialized as `Uri` values,
  - each returned entry MUST be `Uri("s3://<bucket>/<key>")`,
  - when `lazy` is `True`, the method MUST return an iterator,
  - when `lazy` is `False`, the method MUST eagerly realize and return `list[Uri]`,
  - invalid identifier inputs MUST raise the same normalization errors as `parse_uri(...)`,
  - backend listing failures MUST propagate.
- `S3Store.rm(*name_or_uris) -> None`:
  - MUST accept either variadic identifiers or one top-level `list`/`tuple` of identifiers,
  - when no identifiers are supplied, the method MUST return `None` without S3 calls,
  - each identifier MUST normalize through `parse_uri(...)`,
  - delete requests MUST be grouped by bucket,
  - each bucket group MUST be submitted in batches of at most 1000 keys through `delete_objects`,
  - when `delete_objects` returns successfully but reports per-object failures in its response payload, `S3Store.rm(...)` MUST treat the batch request as completed and MUST NOT raise from those embedded failures,
  - the method returns `None`,
  - backend delete failures MUST propagate.
- `S3Store.put_js(data: Any) -> Uri`:
  - MUST serialize `data` with `json.dumps(..., separators=(",", ":"), sort_keys=True)` encoded as UTF-8,
  - MUST store the encoded bytes through `put(..., suffix=".json")`,
  - JSON serialization failures MUST propagate.
- `S3Store.get_js(name_or_uri)`:
  - accepts the same identifier categories as `S3Store.get(...)`: `str`, `Uri`, or `Node`,
  - MUST fetch bytes through `get(...)`,
  - MUST decode as UTF-8 and parse with `json.loads(...)`,
  - MUST return the decoded JSON value produced by `json.loads(...)`,
  - UTF-8 decode failures and JSON parse failures MUST propagate.
- `S3Store.tar(path: str | os.PathLike[str], excludes: Iterable[str] = (), *, symlinks: Literal["ignore", "raise"] = "raise") -> Uri`:
  - `path` MUST identify an existing local directory or the method MUST raise `DmlRepoError`,
  - excludes MUST be matched with `fnmatch` against POSIX-style relative paths from the archive root,
  - `symlinks` MUST accept only `"ignore"` or `"raise"`,
  - when an excluded path is a directory, the entire excluded subtree MUST be pruned,
  - archive traversal order for directory names and file names MUST be sorted,
  - each emitted tar header MUST normalize `uid=0`, `gid=0`, `uname=""`, `gname=""`, and `mtime=0`,
  - non-root directories that remain after exclusion MUST be emitted as directory entries,
  - when `symlinks="raise"`, non-excluded symlinks MUST raise `DmlRepoError`,
  - when `symlinks="ignore"`, non-excluded symlinks MUST be skipped without archive entries,
  - preserved file mode bits MUST come from the source path metadata captured by `tarfile.gettarinfo(...)`,
  - the resulting tar bytes MUST be stored through `put(..., suffix=".tar")`,
  - side effects are local filesystem reads and a single S3 object write.
- `S3Store.untar(tar_uri, dest: str | os.PathLike[str], *, unsafe: bool = False) -> None`:
  - MUST fetch archive bytes through `get(...)`,
  - MUST create `dest` with `parents=True, exist_ok=True` before extraction,
  - when `unsafe` is `False`, the method MUST validate every archive member before extraction,
  - default validation MUST reject any absolute archive path,
  - default validation MUST reject any archive member whose resolved output path escapes the resolved destination root,
  - when default validation fails, extraction MUST fail with `DmlRepoError` before writing archive members,
  - when `unsafe` is `False` and validation succeeds, the method MUST extract all members into `dest`,
  - when `unsafe` is `True`, the method MUST skip the path-containment validation and perform trusted extraction,
  - the method returns `None`.
- `S3Store.cd(new_prefix: str) -> S3Store`:
  - MUST build the current base path as `Path("/" + self.prefix)` when `self.prefix` is non-empty and `Path("/")` otherwise,
  - MUST join `new_prefix` onto that base path and normalize through `Path.resolve().as_posix().lstrip("/")`,
  - the returned store MUST preserve the current bucket and client,
  - the returned store MUST expose the normalized rebased prefix,
  - when normalization resolves to the root path, the returned prefix MUST be the empty string,
  - has no S3 side effects.

### Invariants

- `S3Store.put(...)`, `S3Store.put_js(...)`, and `S3Store.tar(...)` MUST all use the same content-addressed naming rule: SHA-256 of the exact uploaded bytes plus the caller-supplied suffix.
- The default Data Root MUST remain separate from the Protocol Root; deriving the Data Root from `remote.root` MUST produce `<remote.root>/data` semantics, not `<remote.root>/dml` semantics.
- Stored `prefix` values on constructed or rebased `S3Store` instances MUST never retain leading or trailing `/` characters.
- `S3Store.cd(...)` MUST preserve the original store's bucket and client identity.
- `S3Store.cd(...)` rebasing MUST remain consistent with the `Path.resolve().as_posix().lstrip("/")` normalization rule defined for that interface.
- For a given directory tree, exclusion set, file bytes, and file mode bits, `S3Store.tar(...)` MUST produce stable archive bytes independent of source uid, gid, uname, gname, and mtime.
- With `unsafe=False`, `S3Store.untar(...)` MUST never extract an archive member outside the resolved destination directory and MUST never extract an absolute archive path.

### Error Semantics

- Configuration resolution failures:
  - non-retryable until configuration or constructor arguments are corrected,
  - terminal for the current call,
  - caller behavior: pass explicit `bucket`/`prefix` or correct `remote.root`,
  - operator action: fix environment or runtime configuration.
- Local dependency failures during client creation:
  - covers boto3 import failures during default client creation,
  - non-retryable until boto3 installation succeeds,
  - terminal for the current call,
  - caller behavior: do not retry unchanged,
  - operator action: install or repair the Python S3 client dependency.
- Caller input validation failures:
  - includes unsupported identifier types, invalid `remote_root`, invalid `tar(...)` source path, invalid `put(...)` source selection, invalid `tar(..., symlinks=...)` mode, unsupported symlinks when `symlinks="raise"`, and safe-extraction path violations,
  - non-retryable until caller input changes,
  - terminal for the current call,
  - caller behavior: correct the input and invoke again,
  - operator action: none beyond correcting caller logic or input data.
- Object-missing detection:
  - for `exists(...)`, missing-object backend errors with code `404`, `NoSuchKey`, or `NotFound` are non-exceptional and MUST return `False`,
  - for `get(...)`, `get_js(...)`, and `untar(...)`, missing-object failures are terminal for that call unless the backend reports a transient condition instead of a missing-object condition,
  - caller behavior: treat missing data as absent content, not as a retryable success path.
- Backend S3 failures after request dispatch:
  - retryable only when the backend failure is transient,
  - terminal when caused by permanent permission, validation, or missing-resource conditions not mapped to `exists(...) == False`,
  - caller behavior: retry idempotent reads, listings, deletes, and content-addressed writes only when the failure source is transient,
  - operator action: restore backend availability, credentials, or permissions as appropriate.
- Local filesystem failures:
  - includes file-read failures for `put(filepath=...)`, destination-directory creation failures for `untar(...)`, local traversal/read failures for `tar(...)`, and local extraction write failures during `untar(...)`,
  - retryable only when caused by a transient local condition such as temporary filesystem unavailability,
  - terminal when caused by missing paths, permissions, or persistent local corruption,
  - caller behavior: correct the local path or permissions before retrying, and do not assume any partial extraction is recoverable as success,
  - operator action: restore local filesystem availability, permissions, or writable capacity.
- JSON decode and parse failures:
  - non-retryable unless the underlying object bytes are replaced,
  - terminal for the current call,
  - caller behavior: treat as incompatible or corrupt object content,
  - operator action: repair or replace the stored object.
- Embedded per-object delete failures in an otherwise successful `delete_objects` response:
  - non-retryable by the current `S3Store.rm(...)` call because the interface does not inspect or surface those embedded failures,
  - terminal for that call even though some requested objects may remain undeleted,
  - caller behavior: when strict deletion guarantees matter, verify absence with `exists(...)` or a follow-up read and issue a new delete request for any remaining keys,
  - operator action: inspect bucket policy, object-lock rules, versioning behavior, and object ownership for keys that remain.

### Security Boundaries

- `S3Store` trusts the S3 client it is constructed with for credential sourcing, endpoint selection, and request signing; those concerns are outside this document's authority.
- `S3Store.untar(..., unsafe=False)` defines the default local-filesystem safety boundary and MUST reject absolute or destination-escaping archive members.
- `Unsafe Extraction` intentionally disables that local-filesystem safety boundary; callers MUST enable it only for archives they trust.
- This document does not authorize any semantics for IAM policy, credential refresh, encryption policy, or provider-side bucket policy.

### Authority Handoffs

- `Uri` lifecycle and external-data ownership are authoritative in [../storing-and-retrieving-external-data.md](../storing-and-retrieving-external-data.md).
- Configuration key names and `remote.root` syntax are authoritative in [../configuration.md](../configuration.md).
- Repository-managed Protocol Root layout, transport namespaces, CAS layout, and refs layout are authoritative in [../remote-data-model.md](../remote-data-model.md).
- Remote push/pull and other repository protocol behavior are authoritative in [../remote-protocol.md](../remote-protocol.md).
- Contrib adapter/executor runtime behavior that uses `S3Store` artifacts is authoritative in [runtime-contract.md](runtime-contract.md) and [executor-catalog.md](executor-catalog.md).
- The provider SDK behavior behind the default boto3 S3 client is authoritative in the boto3 S3 client documentation.

## Compatibility

- For `status: specified`, the callable names, accepted identifier categories (`str`, `Uri`, `Node` where specified), default Data Root derivation rule, content-address naming rule, `.json` JSON-helper suffix, `.tar` archive-helper suffix, and default safe-extraction behavior are stable compatibility commitments.
- Backward-compatible changes MAY add new utility interfaces or broaden accepted inputs only when existing call signatures and normative behaviors in this document remain unchanged.
- Forward compatibility is not guaranteed for unknown callable arguments, unknown provider error shapes, or undocumented helper behavior; callers MUST rely only on the interfaces and semantics stated in this document.
- This specification uses document revision, not an in-band runtime version field; any change that would alter the stable commitments above is a breaking spec revision and requires an explicit compatibility update to this section.
- Changing the hash algorithm, changing default-root derivation from `<remote.root>/data`, or routing default `S3Store` writes into the Protocol Root would be a compatibility-breaking change.
- `S3Store.ls(...)` compatibility guarantees cover object-entry enumeration only; callers MUST NOT rely on synthetic directory entries because the interface does not return them.
- `Unsafe Extraction` is opt-in and intentionally weakens safety checks; preserving the default `unsafe=False` behavior is a compatibility requirement.
- Forward compatibility is limited to additive changes that preserve existing callable names and current argument semantics; callers MUST NOT assume future interfaces will accept unknown parameters, preserve unknown data, or surface richer delete-status payloads through current return values.

## References

- [../configuration.md](../configuration.md)
- [../remote-data-model.md](../remote-data-model.md)
- [../remote-protocol.md](../remote-protocol.md)
- [../storing-and-retrieving-external-data.md](../storing-and-retrieving-external-data.md)
- [runtime-contract.md](runtime-contract.md)
- [executor-catalog.md](executor-catalog.md)
- https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html
