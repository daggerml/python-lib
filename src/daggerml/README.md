# DaggerML Python Package

`api.py` exposes Python DAG and node authoring; `_cli.py` derives the `dml`
command from the public `Dml` surface. `contrib/` contains optional adapters,
executors, codecs, and extension helpers. `_core/` owns repository semantics;
callers use its package-level public exports rather than its implementation
submodules.

Use the flat [Python authoring](../../docs/use/python-authoring.qmd) and
[CLI](../../docs/use/cli.qmd) references for user-facing behavior. Normative
repository and runtime contracts are mapped in
[`openspec/spec-overview.md`](../../openspec/spec-overview.md).
