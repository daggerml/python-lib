## Why

Dagclass compilation currently rejects class-defined members solely because they are Python-callable, even when normal DaggerML codec normalization supports them. In particular, every `Node` and `Projection` is callable at the Python level, so valid member values can fail before reaching the DAG that can normalize them.

## What Changes

- Restrict dagclass member transformation to recognized Python functions, decorated self-methods, and nested dagclass instances.
- Preserve every other dataclass field or class-defined member as an ordinary namespace value without compile-time callable or descriptor rejection.
- Defer support and error handling for preserved values to normal DAG codec staging.
- Add contract coverage for node, projection, callable codec-backed, and unsupported preserved member values while retaining existing method compilation behavior.

## Capabilities

### New Capabilities

None.

### Modified Capabilities

- `dagclass-namespace-compilation`: Define classification of transformable members versus preserved values and assign ordinary-value validation to DAG codec staging.

## Impact

- Affects dagclass member collection and compilation in `src/daggerml/contrib/api.py`.
- Extends dagclass contract and integration coverage under `tests/contrib/`.
- Updates dagclass authoring documentation to describe the compilation-versus-staging boundary.
- Introduces no new dependency and does not change codec registration or normalization APIs.
