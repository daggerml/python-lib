## 1. CLI Generator Behavior

- [x] 1.1 Add dynamic constructor parameter metadata in `src/daggerml/_cli.py` using resolved annotations and existing `Annotated` normalization.
- [x] 1.2 Add root classmethod intersection logic that matches parameters only by same name and same resolved base type.
- [x] 1.3 Omit intersected parameters from classmethod command-local argument generation while preserving non-intersecting parameters.
- [x] 1.4 Route intersected constructor/root option values into classmethod invocation keyword arguments.
- [x] 1.5 Keep constructor parser destinations collision-safe while overriding user-visible metavars to hide `_init_` internals.

## 2. Contract Tests

- [x] 2.1 Add CLI contract coverage for a fixture class proving same-name/same-type classmethod parameters intersect dynamically.
- [x] 2.2 Add CLI contract coverage proving same-name/different-type classmethod parameters remain command-local.
- [x] 2.3 Add `Dml.init` grammar coverage proving intersected options such as `remote_root`, `user`, and `config_home` are root-only.
- [x] 2.4 Add help/usage coverage proving constructor-derived metavars do not expose `_INIT_` names.
- [x] 2.5 Add dispatch coverage proving root option values are passed to classmethods under the original parameter names.

## 3. Documentation And Validation

- [x] 3.1 Update `docs/reference/cli.md` to describe root-only constructor-intersecting classmethod options and adjust examples as needed.
- [x] 3.2 Run targeted CLI contract tests.
- [x] 3.3 Run the repository's required validation for CLI changes per `CONTRIBUTING.md`.
- [x] 3.4 Run `openspec status --change intersect-classmethod-constructor-cli-args` and confirm the change is apply-ready.
