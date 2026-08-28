## Why

The dashboard node inspector currently mixes function execution context, runnable metadata, script source, and the node's returned value in Summary. Researchers need stable Value and Runnable surfaces that distinguish a node's persisted value from the function runnable applied as a function DAG's `argv[0]`.

## What Changes

- Give every node inspector a Value tab and move bounded value inspection out of Summary.
- Give FnNode inspectors and their function-context DAG inspectors a Runnable tab for the function-applied runnable stored at context DAG `argv[0]`.
- Render Runnable values in the Value tab with the same stack, entrypoint, script, and prepopulation presentation used by the Runnable tab while preserving the distinct Value meaning.
- Represent runnable stacks from outermost wrapper through successive `sub` values to the innermost entrypoint instead of flattening every runnable found anywhere in function arguments.
- Show bounded Python source when the innermost runnable is a script executor with a trusted persisted `script_uri`; otherwise show a specific explanation for why source is unavailable.
- Show script prepopulation as name, value type, and a node link when the prepopulated node exists in the applied function-context DAG.
- Add trusted node-scoped script reads for Runnable node values without accepting an arbitrary browser-provided URI.

## Capabilities

### New Capabilities

- `dashboard-value-runnable-inspection`: Value and Runnable inspector tabs, typed runnable projections, stack and script presentation, prepopulation links, and trusted script access for function-applied and node-value runnables.

### Modified Capabilities

None.

## Impact

- Dashboard node and DAG read-model responses gain explicit value-type and function-applied runnable projections.
- The dashboard API gains a trusted node-derived script-source read for Runnable values while retaining existing bounded function-DAG script reads.
- The React inspector tab model, Summary content, value preview, runnable stack renderer, empty states, routing, and tests change.
- Dashboard architecture documentation and contract tests change; persisted DaggerML objects, execution behavior, and public Python APIs remain unchanged.
