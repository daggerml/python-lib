## Context

`Dml.runtime.describe_graph()` already exposes the execution graph as a raw `ExecutionGraph` payload. That is the right core contract for automation, but it leaves human inspection to ad hoc JSON reading. We want a built-in visual mode without changing the underlying execution-state traversal model and without introducing custom CLI special cases.

## Goals / Non-Goals

**Goals:**
- Add an optional visual mode to `Dml.runtime.describe_graph`.
- Preserve the existing raw payload behavior by default.
- Keep execution-state code responsible only for graph extraction.
- Reuse normal CLI result handling by returning `None` from the visual path.
- Use `rich` as an optional dependency loaded only when visual rendering is requested.

**Non-Goals:**
- Changing `ExecutionState.describe_graph()` payload shape or traversal rules.
- Making `_cli.py` overload-aware.
- Adding new persistent execution metadata.
- Writing user docs that duplicate method docstrings.

## Decisions

### Visual mode lives on the public runtime method

`Dml.runtime.describe_graph(*roots, visual=False)` remains the single caller-facing entrypoint. Callers choose between raw data and rendered output with one boolean option instead of a second method name.

Alternative considered:
- Add a separate `render_describe_graph`-style helper. Rejected because it splits one workflow across two public entrypoints.

### `exec_state.py` stays data-only

`ExecutionState.describe_graph()` continues to return only an `ExecutionGraph`. Rendering happens after the graph is fetched.

Alternative considered:
- Put rendering into `exec_state.py`. Rejected because presentation is not execution-state responsibility.

### The visual path returns `None`

When `visual` is `True`, the runtime method renders directly and returns `None`. This preserves the current CLI machinery: the generated CLI already prints nothing for `None`, so no `_cli.py` change is needed.

Alternative considered:
- Add overload-aware CLI serialization. Rejected as unnecessary complexity for this v0 change.

### `rich` is optional and lazy-loaded

The rendering path imports `rich` only when needed. Missing `rich` should fail clearly at call time with an installation hint.

Alternative considered:
- Add `rich` as a required dependency. Rejected because raw graph inspection should remain lightweight.

### Visual output should respect graph shape

The renderer should not imply the execution graph is always a strict tree. If a node is revisited through another root or edge, the output should mark that fact rather than duplicating it as if it were a new execution.

Alternative considered:
- Render a naive tree. Rejected because it can misrepresent shared reachable nodes.

## Risks / Trade-offs

- Graph-oriented data can look noisy in a visual renderer -> keep the first version compact and lifecycle-focused.
- Optional dependency failures can surprise callers -> raise a direct install hint only on the visual path.
- Direct rendering from the runtime method mixes data and presentation at the top layer -> keep the rendering helper narrowly scoped and out of execution-state code.

## Migration Plan

This is an additive v0 API change.

- Add the `visual` flag and renderer.
- Keep `visual=False` as the default behavior.
- Add the optional `rich` dependency.
- Add tests for raw-return and visual-render paths.

Rollback is straightforward: remove the visual flag and optional dependency without touching execution-state storage.

## Open Questions

- None.
