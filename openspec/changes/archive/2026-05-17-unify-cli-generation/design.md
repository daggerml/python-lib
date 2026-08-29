## Context

The current `dml` CLI is implemented as a package of hand-written parser modules that manually mirror a public `Dml` surface that is already strongly documented with signatures, docstrings, and `Annotated` metadata. That duplication makes it easy for the CLI to drift from `Dml`, requires repeated parser maintenance for every new public method, and leaves some public workflows unexposed. At the same time, some public `Dml` parameters exist for in-process embedding rather than for command-line use, so the generated CLI needs a filtering rule rather than a naive expose-everything rule.

## Goals / Non-Goals

**Goals:**
- Replace the `_cli/` package with one generated `src/daggerml/_cli.py` entrypoint.
- Build command trees from public `Dml` methods and public namespace methods.
- Generate argument parsing from runtime-visible signatures, type hints, defaults, and `Annotated` help text.
- Expose all public methods whose parameter types can be generated from the CLI.
- Keep all CLI output and normalized errors as JSON.
- Move S3 client ownership into `Dml` instances so sync commands remain CLI-exposable.

**Non-Goals:**
- Supporting every possible Python type at the CLI boundary.
- Representing multiple overload variants as distinct CLI command grammars.
- Preserving the current help text formatting or exact command parser structure.
- Adding new domain workflows beyond those already present on the public `Dml` surface.

## Decisions

### Single generated CLI module
The CLI will move to one `src/daggerml/_cli.py` module that owns parser generation, dispatch, JSON serialization, error normalization, and top-level runtime override flags.

Alternatives considered:
- Keep `_cli/*` and add generator helpers: rejected because it preserves the manual duplication problem.
- Generate code ahead of time: rejected because runtime introspection is already available and easier to keep in sync.

### Command tree comes from public `Dml` structure
Top-level commands come from public `Dml` callables plus selected class entrypoints such as `Dml.init`. Public namespace objects such as `config`, `runtime`, `dag`, and `admin` become subcommand groups, and their public methods become leaf commands.

Alternatives considered:
- Maintain an allowlist of command names only: rejected because it reintroduces manual CLI drift.
- Expose private helpers: rejected because the CLI contract should remain on the documented public surface.

### Filter methods by CLI-generatable parameter types
The generator will expose only methods whose public parameters can be derived from CLI input. Supported parameter families are scalar primitives, `Ref`, `Literal`, optionals of supported types, and JSON-backed container types. Methods with unsupported parameter annotations such as `Any` will be omitted entirely rather than partially exposed.

Alternatives considered:
- Expose unsupported parameters as raw strings: rejected because it weakens type-driven parsing and invites ambiguous behavior.
- Expose a method while silently dropping unsupported parameters: rejected because it changes public method semantics invisibly.

### Use one runtime-visible signature when overloads are ambiguous
CLI generation will inspect the runtime-visible implementation signature and its resolved annotations. If overloads describe richer variants than the implementation signature can express directly, the generator will pick that one runtime signature and proceed.

Alternatives considered:
- Encode each overload as a separate CLI grammar: rejected as useful but out of scope.
- Fail generation when overloads exist: rejected because many current public methods already use overloads for return typing only.

### Argument and help generation rules are mechanical
Required parameters become positional arguments. Defaulted parameters become options. Boolean defaults preserve current behavior through positive flags for `False` defaults and `--no-...` flags for `True` defaults. Positional names remain snake case, option names become kebab case, and positional argument documentation is included in the parser description/help text because `argparse` does not present it well by default.

Alternatives considered:
- Encode all inputs as options only: rejected because it obscures method signatures.
- Normalize positional names to kebab case: rejected because snake case maps more directly to parameter names.

### JSON is the only CLI output format
All command results and normalized errors will be emitted as JSON, using the existing typed-leaf serialization rules for `Ref`, `Uri`, and related objects.

Alternatives considered:
- Preserve special plain-text commands such as `config get`: rejected by scope decision.

### `Dml` owns its S3 client
`Dml` will initialize `self._s3_client` during construction and remote sync methods will use that stored client instead of public `s3_client` parameters. This keeps `push`, `pull`, and `fetch` publicly callable from the generated CLI without forcing unsupported parameter filtering on those methods.

Alternatives considered:
- Keep `s3_client` in the signature and special-case it in CLI generation: rejected because it pollutes the public surface with a non-CLI concern.
- Instantiate a new client inside every sync method call: rejected because the current surface already allows shared client reuse and the proposal explicitly prefers instance ownership.

## Risks / Trade-offs

- Broad CLI grammar change -> Mitigation: document this as a breaking CLI redesign and cover representative commands in CLI tests.
- Signature-driven generation may expose awkward public method names directly -> Mitigation: treat the public `Dml` surface as the canonical CLI contract for this redesign.
- Filtering unsupported methods may hide workflows users expect -> Mitigation: document the filtering rule in specs and keep the supported type set explicit.
- Adding `_s3_client` changes an existing `Dml` private-state constraint -> Mitigation: update the `unified-dml-surface` capability to make the new private field explicit.
- Runtime-visible overload selection may miss future richer variants -> Mitigation: document the limitation now and leave multi-overload CLI support as future work.

## Migration Plan

- Land the new CLI generator and update the `dml` entrypoint to import `daggerml._cli:cli` from the new module path.
- Remove `_cli/*` package modules after equivalent generated coverage exists.
- Update CLI docs and tests to the new generated grammar and JSON-only output.
- Update `Dml` sync methods and their callers to use `self._s3_client`.

## Open Questions

- None for this proposal; the supported-type filter, overload fallback rule, and `_s3_client` ownership model are intentionally fixed by scope.
