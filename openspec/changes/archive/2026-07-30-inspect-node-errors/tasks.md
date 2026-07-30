## 1. Core Resolution and Storage

- [x] 1.1 Change core node resolution to return mutually exclusive datum and error refs, and update all existing resolution consumers.
- [x] 1.2 Make DAG node inspection return hydrated stored errors and add the validated `dml.dag.get_error()` query.
- [x] 1.3 Preserve `start_fn() -> Ref | None` by rejecting error-resolving inputs before argv, cache-key, call-node, or execution creation.
- [x] 1.4 Canonicalize `Error` subclasses to exact base `Error` objects in `Error.from_ex()` and the single `TxnWithValid.put()` storage boundary.

## 2. Public API and CLI

- [x] 2.1 Add transient `api.NodeError` with failed-node context traversal.
- [x] 2.2 Translate low-level returned errors into `NodeError` from `_make_node()` and `Node.value()`.
- [x] 2.3 Expose and verify generated `dml dag get-error ERROR_REF` output using existing Error DML serialization.
- [x] 2.4 Remove `Error` from accepted API literal codec staging so it remains unsupported as a node value.

## 3. Verification and Documentation

- [x] 3.1 Add core and API tests for datum-or-error ref resolution, inspection, contextual high-level failures, rejected failed inputs, and canonical error persistence.
- [x] 3.2 Add CLI tests for failed-node and direct-error inspection serialization.
- [x] 3.3 Update error, DAG/node, CLI, and sharp-bits documentation for persisted error inspection and transient `NodeError` behavior.
- [x] 3.4 Run the targeted test suites and the full relevant test suite.
