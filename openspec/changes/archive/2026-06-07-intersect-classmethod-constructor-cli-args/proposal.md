## Why

The generated CLI currently exposes constructor-derived root options and classmethod-derived command options independently, even when they represent the same typed input. This leaks internal constructor destination names such as `_INIT_REMOTE_ROOT` into help output and creates duplicate public spellings for classmethod configuration inputs such as `init --remote-root`.

## What Changes

- Generate classmethod commands by dynamically intersecting their parameters with `Dml.__init__` parameters by same name and same resolved type.
- Omit intersected classmethod parameters from the command-local parser surface.
- Supply intersected classmethod arguments from the parsed constructor/root option values when invoking the classmethod.
- Preserve non-intersecting classmethod parameters as command-local arguments or options.
- Keep constructor option destinations collision-safe internally while ensuring user-visible metavars do not expose implementation prefixes such as `_init_`.
- **BREAKING**: Classmethod command-local options that duplicate same-name/same-type constructor options are removed from the public command grammar. For example, `dml init --remote-root ...` becomes `dml --remote-root ... init`.

## Capabilities

### New Capabilities

- None.

### Modified Capabilities

- `generated-dml-cli`: classmethod command generation and help output behavior for constructor-intersecting parameters.
- `cli-thin-interface`: removes the previous requirement that `init` keep a command-local `--remote-root` distinct from the root override.

## Impact

- Affected code: `src/daggerml/_cli.py` only.
- Affected tests: CLI contract tests for generated classmethod arguments, constructor metavar visibility, and the `dml init` grammar.
- Affected docs: `docs/reference/cli.md` examples or input parsing notes may need updates if they mention command-local duplicate classmethod options.
- No dependency or persisted data impact.
