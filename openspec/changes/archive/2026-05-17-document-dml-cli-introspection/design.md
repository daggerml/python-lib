## Context

`Dml` is now the shared orchestration boundary for the CLI and public API wrappers, but the surface is still weakly self-described at runtime. Callers can inspect method names and signatures, yet class purpose, method behavior, and parameter intent are mostly absent, which makes `help(...)`, editor assistance, and future introspection-driven tooling less useful than the stabilized surface now allows.

This change is intentionally documentation-oriented rather than behavioral. The motivation is future programmatic CLI derivation from `Dml`, but that generation work is explicitly out of scope here. The immediate job is to make the existing `Dml` surface carry enough structured and human-readable metadata that later tooling can consume it without requiring `_cli` to duplicate command descriptions.

## Goals / Non-Goals

**Goals:**
- Make the shared `Dml` surface and its reachable namespaces meaningfully self-describing through runtime introspection.
- Add concise class docstrings that explain the purpose of `Dml` and each namespace object.
- Add concise method docstrings that explain operation behavior, constraints, and side effects.
- Add `typing.Annotated` metadata to public `Dml` and namespace method parameters so parameter help is available in a machine-readable form.
- Establish one consistent documentation split: defaults in signatures, parameter meaning in `Annotated`, and behavioral context in docstrings.

**Non-Goals:**
- Generating CLI parsers, flags, or command trees from `Dml` in this change.
- Auto-synthesizing docstrings from `Annotated` metadata.
- Renaming underscored namespace classes or otherwise redesigning the public object model.
- Changing runtime behavior, payload shapes, or CLI grammar.

## Decisions

### Docstrings and `Annotated` serve different roles

Class docstrings will describe what a namespace or boundary is for. Method docstrings will describe what the operation does, including any notable constraints or side effects. `Annotated` metadata will document what each user-facing parameter means.

Rationale:
- Python does not automatically merge `Annotated` metadata into docstrings or `help(...)` prose.
- Keeping behavior in docstrings and argument meaning in `Annotated` avoids large repetitive parameter sections while still giving future tooling structured help text.

Alternatives considered:
- Put all parameter documentation in docstrings and skip `Annotated`. Rejected because it leaves future CLI-oriented tooling without structured per-parameter metadata.
- Put all documentation in `Annotated` and keep docstrings minimal or absent. Rejected because class and method purpose would remain poorly expressed for human readers and `help(...)` usage.

### Signature defaults remain the source of truth for defaults

Default values will remain encoded only in the Python signature. `Annotated` metadata may explain the meaning of a defaulted parameter but will not restate the literal default unless an example is needed to clarify accepted forms.

Rationale:
- The signature already exposes optionality and default values in a canonical place.
- Repeating defaults in metadata would create unnecessary drift risk for future introspection consumers.

Alternatives considered:
- Repeat defaults inside `Annotated` help strings. Rejected because it duplicates information already present in the signature and makes later edits easier to miss.

### The metadata scope includes namespaced methods, not just top-level `Dml` methods

This change will cover top-level `Dml` methods and the methods reachable through `dml.config`, `dml.runtime`, `dml.dag`, and `dml.admin` sub-namespaces.

Rationale:
- The future CLI shape maps naturally onto those namespaces, so structured help metadata is only useful if it is applied consistently across the whole public surface.
- Restricting metadata to top-level methods would leave the most CLI-like command groups undocumented at the parameter level.

Alternatives considered:
- Annotate only top-level `Dml` methods for a smaller first pass. Rejected because it would produce an uneven introspection contract and weaken the CLI-generation motivation.

### `Annotated` metadata uses concise string help text

Parameter metadata will use plain string payloads inside `typing.Annotated`, with short examples only where accepted selector forms or URI shapes are genuinely ambiguous.

Rationale:
- Plain strings are simple to read in source and easy for future tooling to consume.
- The current motivation is help text, not a richer schema for parser generation.

Alternatives considered:
- Introduce a structured metadata object for parameters. Rejected because it adds API and maintenance overhead before there is a concrete consumer that requires it.

## Risks / Trade-offs

- [Documentation drift] → Keep docstrings short and focused on behavior while treating signatures and `Annotated` strings as the canonical parameter surface.
- [Over-annotated signatures become noisy] → Use concise help strings and reserve inline examples for ambiguous selector or URI parameters.
- [Future CLI generation may want richer metadata] → Start with plain-string `Annotated` values that are easy to migrate or wrap later if a stronger schema becomes necessary.
- [Underscored namespace class names still appear in some introspection output] → Accept that presentation limitation for now and keep the change focused on documentation and metadata.
