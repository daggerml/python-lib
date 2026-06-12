## 1. CLI Result Projection

- [x] 1.1 Update `src/daggerml/_cli.py` root classmethod dispatch to detect `Dml` results and project them to `dml.status()` before output.
- [x] 1.2 Serialize projected bootstrap status payloads using the `Dml.status` return contract instead of the classmethod `-> Dml` annotation.

## 2. Coverage And Docs

- [x] 2.1 Extend `tests/api/contracts/test_cli_classmethod_constructor_contracts.py` to assert `dml init`-style classmethod commands print serialized status payloads.
- [x] 2.2 Add contract coverage for another `Dml`-returning root classmethod path such as `from-config-vars` so the projection is not tied to one command name.
- [x] 2.3 Update `docs/reference/cli.md` to document that root `Dml` classmethod constructors print repository status payloads.

## 3. Verification

- [x] 3.1 Run the targeted CLI contract tests covering classmethod constructor output.
