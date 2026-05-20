## Context

`Dml` currently overloads strings to mean both selectors and exact DB identities. The sharpest examples are DAG and node access methods that accept either `Ref` objects or ref-like strings, plus payloads that return both `id` and `ref` for the same DB-backed object.

This change keeps string-based lookup where the underlying value is not itself a DB object, and requires `Ref` everywhere else. That preserves ergonomic lookup APIs while making exact-object APIs uniform.

## Goals / Non-Goals

**Goals:**
- Make `Ref` the only exact-input and exact-output contract for DB-backed objects on the `Dml` surface.
- Keep selector-style strings for revisions, names, branches, tags, remote URIs, and `index_id` values.
- Split lookup behavior from dereference and mutation behavior so signatures communicate intent.
- Remove duplicated raw DB `id` payload fields where `Ref` already identifies the object.

**Non-Goals:**
- Converting runtime indexes from string ids to `Ref` objects.
- Removing selector-based repository workflows such as revision, branch, or DAG-name lookup.
- Changing lower-level storage identity rules or namespace formats.

## Decisions

### 1. Exact DB object contracts use `Ref` only
Methods that operate on exact DB-backed objects will require `Ref` and will no longer coerce `"ns:..."` strings into refs.

Why:
- It removes ambiguous string contracts.
- It aligns exact-input methods with the typed storage model.

Alternative considered:
- Keep `str | Ref` and document stricter meaning. Rejected because the runtime contract would still be ambiguous at the call site.

### 2. Selector contracts remain string-based
Methods whose job is lookup or navigation will continue accepting strings for revisions, names, branches, tags, remote URIs, and `index_id` values.

Why:
- Those values are not DB objects.
- They are naturally human-authored selectors.

Alternative considered:
- Introduce new wrapper types for selectors. Rejected as unnecessary surface expansion.

### 3. `Dml` separates lookup from dereference
Lookup methods may accept selector strings and return refs. Dereference and mutation methods will accept refs directly.

Why:
- It makes the API boundary legible.
- It gives `daggerml.api` a clean place to remain ergonomic while `_internal.Dml` stays strict.

Alternative considered:
- Preserve combined lookup-and-dereference methods. Rejected because they force selector parsing into object-read APIs.

### 4. DB-backed payload identity uses `ref`, not duplicate `id`
Payloads for commits, DAGs, nodes, and other DB-backed objects will expose `Ref` as the canonical identity and drop duplicate raw `id` fields. Non-DB handles like `index_id` remain strings.

Why:
- It removes two ways to identify the same object.
- It matches the input-side contract.

Alternative considered:
- Keep both `id` and `ref` for convenience. Rejected because it preserves the exact ambiguity this change is trying to eliminate.

## Risks / Trade-offs

- [Breaking callers that pass `.to` strings] -> Update `daggerml.api`, CLI routing, and contract tests together.
- [More explicit multi-step lookup flows] -> Keep selector-oriented methods that return refs so callers can compose lookup and dereference clearly.
- [Spec drift between `_internal.Dml` and higher-level wrappers] -> Make `daggerml.api` explicitly responsible for ergonomic lookup composition.

## Migration Plan

1. Narrow `Dml` and `dml_resolution` signatures and runtime validation.
2. Update payload shaping so DB-backed objects stop returning duplicate raw ids.
3. Update `daggerml.api` and tests to pass `Ref` directly instead of `.to` strings.
4. Update docs and generated help text to describe the new contract.

Rollback is straightforward: restore the previous coercion paths and payload fields if downstream compatibility issues surface before release.

## Open Questions

- Which selector-returning helpers should remain on `_internal.Dml` versus be pushed entirely into `daggerml.api` convenience flows?
- Whether commit payloads should add a `ref` field or rely only on existing commit-typed fields returned from selector workflows.
