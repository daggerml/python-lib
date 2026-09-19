## 1. Eager Funk Source Capture

- [x] 1.1 Capture canonical script and function selection during callable funkification
- [x] 1.2 Make script execution consume captured source with compatibility fallback
- [x] 1.3 Add focused tests for eager timing, explicit injections, isolation, and existing execution behavior

## 2. Hybrid Documentation Execution

- [x] 2.1 Add Jupyter to isolated build tooling without changing published runtime dependencies
- [x] 2.2 Generate engine-specific hidden page setup for project environment and working directory
- [x] 2.3 Extend policy validation and tests for supported mixed engines and incompatible language cells

## 3. Inline Lessons

- [x] 3.1 Convert the Funks lesson to inline Jupyter cells and remove its duplicate injected source
- [x] 3.2 Convert the Dagclasses lesson to inline Jupyter cells and remove its duplicate injected source
- [x] 3.3 Convert any remaining executable page that eagerly funkifies inline definitions to the source-aware engine

## 4. Verification

- [x] 4.1 Run OpenSpec validation, targeted tests, and lint checks
- [x] 4.2 Build the complete dashboard and confirm the inline Jupyter lessons execute successfully
