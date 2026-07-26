# Projects

A DaggerML project is a directory with `.dml/` local state: its database, current checkout, and project configuration. Initialize it with `dml init`; use `dml status` and `dml config show` to inspect it.

The CLI owns project administration. Python authoring opens the existing project through its default runtime or an explicit `Dml(project_home=".")`. Do not use `Dml.init(...)` as the usual project-creation path; it is a low-level API described in the [Python reference](../reference/python-authoring.md).

Project state is local until you configure a remote. See [history and remotes](history-remotes.md).
