## 1. Persistent Runtime Form

- [x] 1.1 Add the `FrozenIndex` persistent type with a partial DAG ref and optional frozen message, and register its namespace.
- [x] 1.2 Add atomic index-to-frozen and frozen-to-index transitions that preserve the shared object ID and commit-shaped fields.
- [x] 1.3 Extend local GC root enumeration to retain frozen runtime indexes.

## 2. Runtime Operations

- [x] 2.1 Add `dml.runtime.freeze(index, *, message=None)` and `dml.runtime.unfreeze(index)` public operations returning the replacement runtime ref.
- [x] 2.2 Reject freeze requests for execution-aware function runtimes without changing local runtime state.
- [x] 2.3 Extend runtime list and describe payloads to report active or frozen state, partial DAG refs, and frozen messages.
- [x] 2.4 Ensure cancel, execution graph inspection, and cache invalidation use the common preserved runtime ID for either runtime form.

## 3. Read and CLI Surfaces

- [x] 3.1 Verify frozen partial DAG refs work with existing DAG descriptions, named-node reads, and projections while terminal result access remains unavailable.
- [x] 3.2 Expose generated CLI commands for runtime freeze, unfreeze, list, and describe using runtime refs and optional freeze messages.
- [x] 3.3 Document runtime freezing and resumption in the applicable user and architecture documentation.

## 4. Verification

- [x] 4.1 Add core contract coverage for type persistence, ID-preserving inverse transitions, function-runtime rejection, and GC retention.
- [x] 4.2 Add API and CLI contract coverage for freeze/unfreeze, list/describe state and message, and inspection of a frozen partial DAG.
- [x] 4.3 Add execution coordination coverage showing cancellation, graph inspection, and invalidation retain frozen runtime lineage.
- [x] 4.4 Run the required formatting, type, lint, and non-slow test checks.
