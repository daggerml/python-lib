## 1. Projection Codec

- [x] 1.1 Complete `ProjectionCodec` with the correct `Projection` input contract, active-DAG validation, base-node encoding, ordered builtin `get` replay, and final-ref return behavior.
- [x] 1.2 Register `ProjectionCodec` in the built-in codec list and in isolated API codec test fixtures without adding projection branches to staging or call entrypoints.

## 2. Contract And Integration Coverage

- [x] 2.1 Add codec contract tests for projection recognition, built-in registration, one base import, ordered nested dict/list/slice access replay, and returning the final access ref.
- [x] 2.2 Add recursive normalization coverage showing projections work directly and inside supported collection or call-argument values through the shared codec pipeline.
- [x] 2.3 Add a live-runtime test that obtains a committed projection through `val.context().node_name[...]`, puts it into the active same-`Dml` DAG, verifies its value, and inspects the import-plus-access graph shape.

## 3. Documentation

- [x] 3.1 Update the Python authoring and DAG/node concept documentation with the context-projection reuse workflow and clarify that source traversal remains read-only until codec-driven insertion into a target DAG.
- [x] 3.2 Update the data/codec concept and codec contract documentation to describe built-in `Projection` encoding, same-`Dml` scope, recursive applicability, and import-plus-`get` graph semantics.

## 4. Verification

- [x] 4.1 Run the focused API codec, node contract, and live-runtime integration tests and resolve any regressions.
- [x] 4.2 Run the repository lint and full test commands required by `CONTRIBUTING.md`.
