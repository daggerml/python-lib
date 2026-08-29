# Plugin Registration

Adapters and executors are discovered lazily from installed Python entry points:

| Kind | Entry-point group | Lookup key |
| --- | --- | --- |
| Adapter | `daggerml.contrib.adapters` | adapter name |
| Executor | `daggerml.contrib.executors` | `(adapter, executor name)` |
| Literal codec | `daggerml.codecs` | priority order |
| Custom dashboard | `daggerml.dashboards` | dashboard name |

Adapter and executor entry points load one adapter or executor class/spec. They
are not callback hooks and are not called after loading. The loaded object must
provide the attributes used by its registry: adapters need `name`; executors
need `adapter` and `name`.

The registries load once per process on first lookup or list. A load failure is
raised as `DmlRepoError` and leaves loading incomplete, so a later lookup may
try again. Duplicate adapter names and duplicate executor keys warn and the
last discovered entry wins. Entry-point discovery is sorted for codec plugins;
do not depend on duplicate discovery order for adapters or executors.

Codec entry points are different: they load a factory and call it to obtain
`(priority, codec)` registrations. See [Plugin API](../reference/plugin-api.md).

Custom dashboard entry points also load and call a factory, which returns
ordered named dashboard definitions. Their render functions are trusted local
UI extensions, not adapters or persistent DaggerML executions. Compatibility
uses the selected immutable DAG's intrinsic tags.
