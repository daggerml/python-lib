# Configuration

DaggerML uses one resolved configuration model across the Python API and the CLI.

## Resolved shape

The resolved config returned by the internal resolver has this shape:

```json
{
  "project": {
    "home": "string-or-null"
  },
  "db": {
    "path": "string-or-null"
  },
  "remote": {
    "project": "string-or-null",
    "root": "string",
    "fetch_workers": 16
  },
  "user": "string-or-null",
  "default_branch": "main",
  "config_home": "string"
}
```

Canonical keys:

- `project.home`
- `db.path`
- `remote.project`
- `remote.root`
- `remote.fetch_workers`
- `user`
- `default_branch`
- `config_home`

## Precedence

Resolved values are layered in this order:

1. defaults
2. global config
3. project config for project-scoped resolution
4. environment variables
5. explicit constructor or CLI overrides

Notes:

- Later layers override earlier ones key by key.
- Empty or missing higher-precedence values do not erase lower-precedence values.
- For project-scoped resolution, `project.home` defaults to the current working directory when not provided.

## Environment variables

- `DML_PROJECT_HOME` -> `project.home`
- `DML_DB_PATH` -> `db.path`
- `DML_REMOTE_PROJECT` -> `remote.project`
- `DML_REMOTE_ROOT` -> `remote.root`
- `DML_REMOTE_FETCH_WORKERS` -> `remote.fetch_workers`
- `DML_USER` -> `user`
- `DML_DEFAULT_BRANCH` -> `default_branch`
- `DML_CONFIG_HOME` -> `config_home`

Global config home resolution:

1. `DML_CONFIG_HOME`
2. `$XDG_CONFIG_HOME/dml`
3. `~/.config/dml`

## Repo-local files

Project state lives under `.dml/` inside `project.home`.

- `.dml/config.toml`: repo-local remote settings
- `.dml/db/`: local object database
- `.dml/HEAD`: current checkout state
- `.dml/.gitignore`: created during init

`Dml.init(...)` creates `.dml/`, writes `.dml/.gitignore`, writes `.dml/config.toml` if needed, and creates the database when it does not already exist.

## Field rules

- `default_branch` defaults to `main`.
- `db.path` defaults to `<project.home>/.dml/db` for project-scoped resolution.
- `remote.fetch_workers` must be a positive integer and defaults to `16`.
- `remote.root` must be empty or an `s3://bucket` or `s3://bucket/prefix` URI.
- `remote.project` must be a bare `dml://<owner>/<project>` URI.
- `remote.project` may not include `#branch` or `@tag` in config.

`remote.root` enables remote-backed execution and storage. `remote.project` is the additional setting required for project-addressed sync such as `push`, `pull`, and `fetch` against a configured project.

## Python and CLI entrypoints

Python:

```python
from daggerml import Dml

dml = Dml(
    project_home=".",
    remote_root="s3://my-bucket/demo",
    user="alice@example",
)
```

CLI:

```bash
dml --project-home . --remote-root s3://my-bucket/demo status
dml config show
dml config set remote.root s3://my-bucket/demo
```

## Config file locations and contents

Global config is read from `config.toml` under the resolved config home. The current resolver reads:

- `[user].name`
- `[defaults].branch`
- `[remote].fetch_workers`

Project config is read from `.dml/config.toml`. The current resolver reads:

- `[remote].project`
- `[remote].root`
- `[remote].fetch_workers`

## Related pages

- [CLI](cli.md)
- [Errors](errors.md)
