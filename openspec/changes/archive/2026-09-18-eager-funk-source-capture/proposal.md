## Why

Interactive authoring environments may expose a function's source when it is decorated but no longer expose that source when the delayed runnable is staged. Deferring source inspection until staging therefore makes otherwise valid script funks unreliable.

## What Changes

- Capture a script funk's normalized source and function name when `funkify` receives the callable.
- Make script staging consume the captured source without inspecting the callable again.
- Preserve fallback rendering for delayed runnables created before eager capture.
- Preserve source-only worker isolation: globals, closures, and interpreter state are not implicitly serialized.

## Capabilities

### New Capabilities

- `funk-source-capture`: Defines eager, source-only serialization of funkified functions at authoring time.

### Modified Capabilities

None.

## Impact

The change affects `daggerml.contrib.api.funkify`, script executor lowering, and focused script executor contracts. Source lookup failures now occur during callable funkification instead of later staging.
