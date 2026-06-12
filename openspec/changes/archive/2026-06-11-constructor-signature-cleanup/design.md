## Context

The internal config resolver already works in flattened canonical keys such as `remote.root` and `default.db_map_size_max`, while the public `Dml` constructor exposes a smaller alias set such as `remote_root` and `db_map_size_max`. Python cannot pass dot-notation keys directly as kwargs, so the public API needs two intentionally different shapes: Python-friendly keyword construction and canonical config-dict construction.

## Goals / Non-Goals

**Goals:**
- Let `Dml.__init__` accept the full supported config-var surface via Python-friendly names.
- Add a classmethod that accepts flattened canonical config-var dictionaries directly.
- Make `Dml.init(...)` accept the same config-var surface as `Dml.__init__`, plus init-only args.

**Non-Goals:**
- Defining clone behavior in this change.
- Changing config precedence rules.
- Exposing dot-notation keys directly as Python keyword parameters.

## Decisions

- Keep `__init__` Pythonic and alias-based.
  Rationale: Python kwargs cannot carry canonical dot-notation config keys.
- Add `Dml.from_config_vars(...)` for flattened config-var dictionaries.
  Rationale: it gives callers an explicit canonical-key entrypoint instead of overloading `__init__`.
- Align `Dml.init(...)` with constructor config kwargs and keep bootstrap-only args separate.
  Rationale: repo bootstrap should not have a narrower config surface than session construction.
- Leave clone for a follow-up proposal.
  Rationale: clone adds repo/bootstrap semantics beyond constructor normalization.

## Risks / Trade-offs

- [Constructor signatures affect generated CLI help] -> Keep parameter naming deliberate and test CLI generation contracts.
- [Two constructor shapes can confuse callers] -> Make the split explicit: kwargs for Python, flattened dict for canonical config vars.
- [Adding many kwargs can bloat the surface] -> Restrict additions to supported config vars only.
