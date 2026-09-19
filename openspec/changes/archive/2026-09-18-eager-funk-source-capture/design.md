## Context

`funkify` currently stores a callable in delayed runnable metadata. The script executor later inspects that callable while lowering the delayed runnable. Notebook and REPL source caches are most reliable near definition time, so source that was available during decoration may be unavailable by staging.

## Goals / Non-Goals

**Goals:**

- Capture canonical script source at the earliest public script-authoring boundary.
- Avoid repeated source inspection during staging.
- Preserve explicit source dependencies, postlude source, function selection, and source-only isolation.
- Continue accepting older delayed metadata that contains only a callable.

**Non-Goals:**

- Serializing closures, globals, or interpreter state.
- Changing DAG result, caching, or worker execution semantics.
- Eagerly rendering callables for non-script executors.

## Decisions

### Store canonical source in delayed runnable metadata

When `funkify` receives a callable for the script executor, it renders the canonical script immediately and stores both `script` and `fn_name` alongside the callable. Explicit `extra_objs` and `post_lines` remain the only source additions.

### Prefer captured source during script lowering

The script executor validates and uses a captured `script` and `fn_name` pair without calling source inspection again. Metadata containing only the callable follows the existing deferred-rendering path for compatibility with delayed runnables constructed before this change.

### Reserve captured metadata keys

Callable funkification rejects user-supplied `fn`, `script`, and `fn_name` values so callers cannot conflict with internally generated metadata.

## Risks / Trade-offs

- Source lookup failures move from staging to funkification, which is intentional and covered by focused tests.
- Keeping the callable in delayed metadata duplicates its identity beside canonical source, but preserves existing delayed-runnable behavior while staging switches to the captured representation.
