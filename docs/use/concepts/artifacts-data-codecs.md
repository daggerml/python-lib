# Artifacts, data, and codecs

DaggerML stores ordinary scalar values, lists, dictionaries, `Uri` values, and runnables in DAGs. The current Python API uses codecs to normalize Python values into that stored model. Other language toolchains can define their own value-normalization and codec mechanisms for the same DaggerML model.

Built-in codecs also preserve graph relationships. A `Node` codec reuses a node already in the active DAG or imports a committed node. A `Projection` codec imports its committed base node from the same `Dml` instance and replays the stored path as builtin `get` nodes. Recursive normalization applies these rules whether the value is passed directly, nested in a collection or runnable, or used as a function argument.

Keep large payloads in external storage and commit their `Uri` as an artifact. `S3Store` writes content-addressed data under the project remote's data prefix; built-in optional dataframe codecs can externalize pandas or polars frames as parquet URIs.

Use a custom codec when your Python type needs a repeatable stored representation. See [artifacts](../guides/artifacts.md) and [custom codecs](../guides/custom-codecs.md).
