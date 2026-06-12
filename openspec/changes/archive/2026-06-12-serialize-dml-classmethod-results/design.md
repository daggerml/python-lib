## Context

`src/daggerml/_cli.py` generates root commands directly from `Dml` methods and root classmethods. In `MethodCLI.run()`, instance commands serialize the invoked method result against that method's return annotation, and root classmethod commands follow the same path.

That works for payload-returning commands, but `Dml.init(...)`, `Dml.clone(...)`, and `Dml.from_config_vars(...)` return `Dml` instances. `_serialize_result()` derives serializer families from the invoked method's return annotation, so a raw `Dml` object is not compatible with the existing serializer-map rules. A simple value substitution is also insufficient unless the CLI serializes against the projected status payload contract rather than the classmethod's `Dml` return annotation.

## Goals / Non-Goals

**Goals:**
- Make root `Dml` classmethod commands produce CLI output that reflects the initialized repository state.
- Preserve the generated CLI's existing serializer-family rules for ordinary payloads and typed leaves.
- Keep the behavior dynamic and type-driven rather than special-casing individual command names.

**Non-Goals:**
- Change `Dml` classmethod return types away from `Dml`.
- Add a generic CLI serializer family for arbitrary `Dml` instances.
- Change output behavior for non-classmethod commands.

## Decisions

1. Project root classmethod `Dml` results to `status()` before serialization.

Root classmethod dispatch will detect `isinstance(result, self.cls)` and treat that value as a bootstrap runtime object, then call `result.status()` to obtain the user-facing payload.

Why:
- The user-facing meaning of bootstrap commands is the repository state they created or opened, not the opaque runtime object.
- It keeps `Dml` as the runtime boundary while still giving the CLI a JSON-ready surface.

Alternative considered:
- Register a global serializer for `Dml`. Rejected because it would make `Dml` itself a CLI transport type, which is broader than the desired classmethod bootstrap behavior.

2. Serialize the projected payload using `Dml.status`'s return contract.

After projection, the CLI will serialize the resulting payload using the return annotation contract of `Dml.status` rather than the classmethod's `-> Dml` annotation.

Why:
- `_serialize_result()` selects serializer families from the callable's declared return type.
- Using the classmethod return annotation after projection would still fail because `dict`-like status payloads do not match `Dml`.
- Reusing `Dml.status` keeps serializer behavior aligned with the existing shared runtime status contract.

Alternative considered:
- Bypass `_serialize_result()` and emit raw JSON directly. Rejected because it would duplicate or sidestep the generated CLI's typed-leaf serializer rules.

3. Cover the behavior in the existing classmethod CLI contract suite.

Contract tests will extend the classmethod constructor CLI suite to assert that `dml init`-style commands print serialized status payloads rather than failing or printing object reprs.

Why:
- The affected behavior lives in root classmethod dispatch and output shaping, which is already exercised by the existing classmethod CLI contract area.

## Risks / Trade-offs

- [Future non-bootstrap classmethods may also return `Dml`] -> Scope the projection to root classmethod dispatch plus `isinstance(result, self.cls)` so the behavior is explicit and still generic across current bootstrap constructors.
- [Status payload shape changes would affect bootstrap CLI output] -> Reuse the established `Dml.status()` contract so bootstrap output stays aligned with the shared runtime status surface and existing status tests.
- [A broader serializer hook could be tempting later] -> Keep this change minimal and localized so future serializer expansions remain deliberate spec work.
