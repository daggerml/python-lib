---
status: specified
doc_type: spec
---

# Contrib Testing

## Authority

This document is authoritative for contrib-owned testing helpers in `daggerml.contrib.testing`.

This document owns:

- the testing helper surface currently provided by `daggerml.contrib.testing`,
- the node-like contract exposed by those helpers for contrib author-code unit tests,
- the minimal shape assumptions this helper surface makes about recognized `DelayedRunnable` inputs and recognized real `Node` values.

This document does not own runtime execution behavior, repository-backed `Node` semantics, or generic Python testing practices.

## Scope

In scope:

- testing helpers implemented in `src/daggerml/contrib/testing.py`,
- unit-test support for contrib author code that expects node-like `.value()` access,
- recognition rules for when this helper surface preserves an existing real `Node` or accepts a delayed-runnable wrapper chain.

Out of scope:

- repository-backed DAG execution,
- `Node` persistence, identity, refs, or transactional behavior,
- helpers beyond the currently implemented testing surface.

## Purpose

Define the minimal contrib testing surface for unit tests that need contrib-facing node semantics without requiring a repository or database-backed runtime.

## Glossary

- **MockNode**: the contrib testing helper that wraps one Python value and exposes node-like `.value()` access.
- **`MockNode.from_value(...)`**: the `MockNode` class helper that preserves real `Node` and `MockNode` inputs unchanged and otherwise returns a new `MockNode`.
- **Defunkified Callable**: a wrapper returned by `defunkify(...)` that exposes the innermost script callable from a `DelayedRunnable` and auto-wraps plain test inputs as node-like values.
- **Node-Like Test Value**: a testing helper object intended to satisfy only the contrib-facing `.value()` protocol used by author code.
- **Author Code Unit Test**: a test that exercises user-authored contrib callables or dagclass methods without repository-backed DAG execution.
- **DelayedRunnable**: the contrib delayed-runnable wrapper defined in [api.md](api.md).
- **DAG**: the repository-backed directed acyclic graph model defined in [../dag-model.md](../dag-model.md).
- **Node**: the repository-backed public node wrapper defined in [../api.md](../api.md) and [../object-model.md](../object-model.md).

## Contract

### Interfaces

#### Location
- `daggerml.contrib.testing`

#### Testing Helper Surface
- `MockNode`
- `MockNode.from_value(...)`
- `defunkify(...)`

#### `MockNode`
- **Location/Name**: `MockNode` class.
- **Signature/Schema**: `MockNode(value: object)`
- **Accepted Inputs and Output Shape**: Accepts exactly one wrapped Python value. Output is a `MockNode` instance.
- **Behavior/Semantics**: It is a Node-Like Test Value only. It MUST be usable in Author Code Unit Tests that rely only on `.value()` access. It MUST NOT require repository state, database state, or active repository-backed runtime configuration. It MUST NOT claim or emulate repository-backed `Node` identity, refs, persistence, DAG membership, or transactional behavior.
- **Errors and Failure Modes**: Extra positional or keyword construction arguments are rejected.
- **Side Effects**: None.
- **Constraints**: Single argument only.
- **Invocation Surfaces**: Python API.
- **Unspecified Fields**: Rejected.

#### `MockNode.from_value(...)`
- **Location/Name**: `MockNode.from_value` class method.
- **Signature/Schema**: `from_value(value: object) -> MockNode | Node`
- **Accepted Inputs and Output Shape**: Accepts one value. Returns `value` unchanged when it is already a `Node` or `MockNode`, otherwise returns `MockNode(value)`.
- **Behavior/Semantics**: Preserves real `Node` and `MockNode` inputs.
- **Errors and Failure Modes**: Extra positional or keyword arguments are rejected.
- **Side Effects**: None.
- **Constraints**: Single argument only.
- **Invocation Surfaces**: Python API.
- **Unspecified Fields**: Rejected.

#### `MockNode.value()`
- **Location/Name**: `MockNode.value` instance method.
- **Signature/Schema**: `value() -> object`
- **Accepted Inputs and Output Shape**: Takes no arguments. Returns the wrapped value unchanged.
- **Behavior/Semantics**: Provides node-like value access.
- **Errors and Failure Modes**: Extra positional or keyword arguments to `value()` are rejected.
- **Side Effects**: None.
- **Constraints**: No arguments.
- **Invocation Surfaces**: Python API.
- **Unspecified Fields**: Rejected.

#### `defunkify(...)`
- **Location/Name**: `defunkify` function.
- **Signature/Schema**: `defunkify(delayed: DelayedRunnable) -> callable`
- **Accepted Inputs and Output Shape**: Input MUST be a `DelayedRunnable`. Supported input is limited to delayed-runnable wrapper chains whose innermost delayed runnable is script-backed and retains the original callable in `kwargs["fn"]`. Output is a callable.
- **Behavior/Semantics**: `defunkify(...)` MUST be the supported testing/debug path for recovering script callables from delayed runnable wrapper chains. For supported input, it MUST traverse nested delayed-runnable wrappers to the innermost script-backed delayed runnable before constructing the returned callable. The returned callable MUST execute inside an isolated temporary working directory rather than the caller's ambient current working directory. The returned callable MUST preserve the leading positional argument unchanged. The returned callable MUST bind defaults and MUST wrap every non-leading bound argument value that is not already a real `Node` or `MockNode` into `MockNode` before invoking the original callable. The returned callable MUST preserve `Node` and `MockNode` inputs rather than rewrapping them. The returned callable MUST preserve the wrapped callable's accepted argument names and defaulting behavior.
- **Errors and Failure Modes**: Extra positional or keyword arguments to `defunkify(...)` are rejected. It MUST reject unsupported delayed-runnable inputs rather than guessing or synthesizing callable metadata. The returned callable MUST reject invocation patterns that the wrapped callable's signature does not accept.
- **Side Effects**: Creates a temporary working directory during the invocation of the returned callable.
- **Constraints**: Only supports script-backed runnables.
- **Invocation Surfaces**: Python API.
- **Unspecified Fields**: Delayed-runnable metadata unrelated to locating the supported innermost script callable is preserved on the input value and ignored by this helper.

### Invariants

- `MockNode.value()` MUST return the same wrapped value for the lifetime of the `MockNode` instance.
- `MockNode.from_value(...)` MUST preserve existing `Node` and `MockNode` inputs without rewrapping them.
- `defunkify(...)` MUST preserve already-node-like argument values without rewrapping them.
- `defunkify(...)` MUST restore the caller's original current working directory after invocation, including when the wrapped callable raises.
- `MockNode` MUST remain side-effect free.
- `MockNode` MUST provide only node-like testing behavior within this document's authority; tests requiring real DAG or runtime semantics MUST use repository-backed APIs instead.

### Error Semantics

#### Invalid `MockNode`/`MockNode.from_value(...)`/`MockNode.value()` invocation shape
- **Retryable or non-retryable**: Non-retryable until the call shape is corrected.
- **Transient vs terminal**: Terminal and non-transient for that helper invocation.
- **Required caller behavior**: Call the helper with the documented positional-only shape for that interface.
- **Required operator action**: Fix the test call site; no runtime remediation exists.

#### `defunkify(...)` with non-`DelayedRunnable` input
- **Retryable or non-retryable**: Non-retryable until the input value is corrected.
- **Transient vs terminal**: Terminal and non-transient for that helper invocation.
- **Required caller behavior**: Pass a supported `DelayedRunnable` wrapper chain instead of a plain object.
- **Required operator action**: Fix the test/debug call site so it supplies a delayed-runnable wrapper chain.

#### `defunkify(...)` with an innermost delayed runnable whose `uri` is not `"script"`
- **Retryable or non-retryable**: Non-retryable until the input value is corrected.
- **Transient vs terminal**: Terminal and non-transient for that helper invocation.
- **Required caller behavior**: Pass a delayed-runnable wrapper chain whose innermost runnable is script-backed.
- **Required operator action**: Fix the helper input selection so `defunkify(...)` is used only for script-backed delayed-runnable chains.

#### `defunkify(...)` with missing or non-callable innermost `kwargs["fn"]`
- **Retryable or non-retryable**: Non-retryable until the input value is corrected.
- **Transient vs terminal**: Terminal and non-transient for that helper invocation.
- **Required caller behavior**: Pass a delayed-runnable wrapper chain that retains a callable innermost `kwargs["fn"]`.
- **Required operator action**: Fix the delayed-runnable construction or selection so callable metadata is retained.

#### Invalid invocation of the callable returned by `defunkify(...)`
- **Retryable or non-retryable**: Non-retryable until the call shape is corrected.
- **Transient vs terminal**: Terminal and non-transient for that invocation.
- **Required caller behavior**: Call the returned wrapper with arguments accepted by the wrapped callable's signature.
- **Required operator action**: Fix the test call site; no runtime remediation exists.

#### Misuse of `MockNode` as a repository-backed `Node` substitute
- **Retryable or non-retryable**: Non-retryable within this helper surface.
- **Transient vs terminal**: Terminal and non-transient for tests that require real DAG/runtime semantics.
- **Required caller behavior**: Use repository-backed APIs when tests require DAG membership, refs, or execution semantics.
- **Required operator action**: Switch the test to repository-backed APIs or reduce the test to contrib-facing `.value()` behavior only.

### Authority Handoffs

- `funkify`, `dagclass`, and delayed-runnable behavior are authoritative in [api.md](api.md).
- Repository-backed `DAG` semantics are authoritative in [../dag-model.md](../dag-model.md).
- Repository-backed `Node` semantics are authoritative in [../api.md](../api.md) and [../object-model.md](../object-model.md).
- Runtime execution behavior is authoritative in [runtime-contract.md](runtime-contract.md).

## Compatibility

- This document defines versionless, `status: specified` contracts for `MockNode`, `MockNode.from_value(...)`, and `defunkify(...)`.
- Backward compatibility guarantees:
  - `MockNode(value).value()` MUST keep returning the original wrapped value unchanged,
  - `MockNode.from_value(...)` MUST keep preserving `Node` and `MockNode` inputs unchanged,
  - `defunkify(...)` MUST keep preserving the leading positional argument and wrapping only non-leading non-node inputs into `MockNode` for supported inputs.
- Forward compatibility guarantees:
  - callers MAY ignore newly added helpers that are documented in a future revision of this document,
  - callers MUST NOT rely on undocumented helper names, extra methods, or broader `Node` emulation than this document specifies,
  - adding new testing helpers or new optional helper methods is forward-compatible only when existing specified behavior remains unchanged and this document is updated in the same change.
- Versioning guarantees:
  - this helper surface has no separate runtime-negotiated version field,
  - compatibility-relevant behavior changes require updating this document and the published package version together,
  - changing `MockNode.value()` to transform, copy, or otherwise alter the wrapped value is a breaking contract change.

## References

- [api.md](api.md)
- [../api.md](../api.md)
- [../dag-model.md](../dag-model.md)
- [../object-model.md](../object-model.md)
- [runtime-contract.md](runtime-contract.md)
