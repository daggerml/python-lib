# DaggerML Python Package

`api.py` exposes Python DAG and node authoring; `_cli.py` derives the `dml`
command from the public `Dml` surface. `contrib/` contains optional adapters,
executors, codecs, and extension helpers. `_core/` owns repository semantics;
callers use its package-level public exports rather than its implementation
submodules.

Use the executable [Start here](../../docs/start-here/index.qmd) course for
authoring and the ordered [Use](../../docs/use/projects.qmd) course for project,
artifact, execution, inspection, runtime, and sharing workflows. Normative
repository and runtime contracts are mapped in
[`openspec/spec-overview.md`](../../openspec/spec-overview.md).
