## 1. Runtime Surface

- [x] 1.1 Extend `Dml.runtime.describe_graph` in `src/daggerml/_core/dml.py` with `visual: bool = False` while preserving the current raw graph return path.
- [x] 1.2 Add a visual rendering path that renders the fetched execution graph and returns `None` when `visual=True`.

## 2. Rendering And Packaging

- [x] 2.1 Add a small rendering helper outside `exec_state.py` that produces a human-friendly graph view without implying the graph is always a strict tree.
- [x] 2.2 Add `rich` as an optional dependency and make the visual path import it lazily with a clear missing-dependency error.

## 3. Verification

- [x] 3.1 Add or update runtime surface tests covering `describe_graph()` raw return behavior and `describe_graph(..., visual=True)` returning `None`.
- [x] 3.2 Add or update CLI-facing behavior tests showing the generated command still emits structured output for the raw path and no parsed output for the visual path.
- [x] 3.3 Run the relevant targeted tests for runtime and CLI behavior.
