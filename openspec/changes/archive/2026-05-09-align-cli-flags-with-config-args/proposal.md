## Why

The CLI still exposes flag names such as `--repo` and `--remote-root` even though the shared configuration model is defined in canonical names like `project.home` and `remote.uri`. This mismatch makes the CLI harder to learn, weakens the contract between docs and implementation, and causes help text, error hints, and tests to drift away from the configuration model they are supposed to expose.

## What Changes

- Rename CLI flags so user-facing option names mirror their canonical configuration keys where practical, including replacing `--repo` with `--project-home` and `--remote-root` with `--remote-uri`.
- Update command help text, examples, and normalized CLI error hints to use the canonical flag names.
- Update init command inputs and any other CLI-exposed overrides so they consistently use config-shaped names already defined by the shared resolver.
- Update contract and integration tests to cover the renamed flags and reject stale flag names once the rename lands.
- **BREAKING**: Remove the old CLI flag spellings where they conflict with the canonical config naming contract.

## Capabilities

### New Capabilities
<!-- None. -->

### Modified Capabilities
- `cli-thin-interface`: The CLI surface will change its public flag names while preserving thin delegation behavior and output structure.
- `shared-internal-configuration`: CLI explicit-argument naming will align with the canonical config keys exposed by the shared resolver.

## Impact

- Affected code: `src/daggerml/_cli/__init__.py`, `base.py`, `init.py`, `status.py`, `config.py`, `remote.py`, related internal error messages, and CLI-facing docs.
- Affected tests: CLI contract and integration tests that parse or invoke the renamed flags.
- Affected users: anyone invoking the CLI with `--repo` or other non-canonical override names.
- Special implementation concern: the top-level `--remote-uri` rename will overlap with `init --remote-uri`, so parser destinations must remain distinct even if the public flag spelling is shared.
- No new runtime dependencies are expected.
