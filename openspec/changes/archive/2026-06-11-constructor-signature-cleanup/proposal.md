## Why

`Dml.__init__` currently accepts only a curated alias set even though the underlying config system has a broader canonical config-var surface. That split makes constructor, init, and config-dict use cases diverge unnecessarily and forces callers to know which entrypoint accepts which shape.

## What Changes

- Expand the `Dml` constructor surface to accept all supported config vars through Python-friendly parameter names.
- Add a config-dict constructor for flattened canonical config-var dictionaries.
- Align `Dml.init(...)` with the same config-var surface, plus init-only arguments.

## Capabilities

### New Capabilities

### Modified Capabilities
- `unified-dml-surface`: the `Dml` constructor and classmethod surface will expand.
- `shared-internal-configuration`: canonical flattened config vars will gain a direct `Dml` factory entrypoint.

## Impact

- Affected code: `src/daggerml/_core/dml.py`, `src/daggerml/_core/config.py`, CLI constructor generation, API helper tests.
- Affected behavior: Python constructor signatures, config-dict instantiation, init signature alignment.
- No repo-model change by itself.
