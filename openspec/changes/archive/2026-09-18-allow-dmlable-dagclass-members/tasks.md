## 1. Member Classification

- [x] 1.1 Update dagclass class-member collection to compile only plain functions and decorated self-methods, replace nested dagclass instances, and preserve every other evaluated value without generic callable or descriptor rejection.
- [x] 1.2 Add contract tests proving dataclass fields and class-defined ordinary values share the same classification while existing plain, decorated, and nested dagclass compilation remains intact.

## 2. Codec Staging Boundary

- [x] 2.1 Add coverage showing class-defined `Node`, `Projection`, and callable custom-codec values survive instantiation and normalize through the normal DAG codec path when staged.
- [x] 2.2 Add coverage showing unsupported preserved values and contextually invalid cross-index nodes fail during staging with their normal codec errors rather than during dagclass compilation.

## 3. Documentation And Verification

- [x] 3.1 Update dagclass authoring documentation to distinguish recognized compilation forms from ordinary values validated by codec staging.
- [x] 3.2 Run formatting, type checks, targeted dagclass tests, and the required non-slow test suite.
