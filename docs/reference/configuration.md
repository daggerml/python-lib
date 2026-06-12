# Configuration

DaggerML uses one resolved configuration model across the Python API and the CLI.

## Resolved shape

The resolved config returned by the internal resolver has this shape:

```json
{
  "project_home": "string",
  "db_path": "string",
  "default": {
    "db_map_size_headroom": 1048576,
    "db_map_size_max": 10737418240,
    "branch_name": "main"
  },
  "remote": {
    "project": "string-or-null",
    "root": "string-or-null",
    "prune_age_seconds": 86400,
    "fetch_workers": 32
  },
  "user": "string-or-null",
  "config_home": "string"
}
```

Canonical keys:

- `project_home`
- `db_path`
- `default.db_map_size_headroom`
- `default.db_map_size_max`
- `default.branch_name`
- `remote.prune_age_seconds`
- `remote.project`
- `remote.root`
- `remote.fetch_workers`
- `user`
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
- For project-scoped resolution, `project_home` defaults to the current working directory when not provided.

## Environment variables

- `DML_PROJECT_HOME` -> `project_home`
- `DML_DB_PATH` -> `db_path`
- `DML_DEFAULT_DB_MAP_SIZE_HEADROOM` -> `default.db_map_size_headroom`
- `DML_DEFAULT_DB_MAP_SIZE_MAX` -> `default.db_map_size_max`
- `DML_DEFAULT_BRANCH_NAME` -> `default.branch_name`
- `DML_REMOTE_PROJECT` -> `remote.project`
- `DML_REMOTE_ROOT` -> `remote.root`
- `DML_REMOTE_PRUNE_AGE_SECONDS` -> `remote.prune_age_seconds`
- `DML_REMOTE_FETCH_WORKERS` -> `remote.fetch_workers`
- `DML_USER` -> `user`
- `DML_CONFIG_HOME` -> `config_home`

Global config home resolution:

1. `DML_CONFIG_HOME`
2. `$XDG_CONFIG_HOME/dml`
3. `~/.config/dml`

## Repo-local files

Project state lives under `.dml/` inside `project_home`.

- `.dml/config.json`: repo-local remote settings
- `.dml/db/`: local object database
- `.dml/HEAD`: current checkout state
- `.dml/.gitignore`: created during init

`Dml.init(...)` creates `.dml/`, writes `.dml/.gitignore`, writes `.dml/config.json` if needed, and creates the database when it does not already exist.

`Dml.clone(...)` uses the same bootstrap layout, persists a branchless `remote.project` derived from the clone source URI, fetches the selected branch or tag, and then leaves `HEAD` attached for branch clones or detached for tag clones.

## Field rules

- `default.branch_name` defaults to `main`.
- `db_path` defaults to `<project_home>/.dml/db` for project-scoped resolution.
- `remote.fetch_workers` must be a positive integer and defaults to `32`.
- `remote.prune_age_seconds` must be a positive integer and defaults to `86400`.
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
    remote_fetch_workers=8,
    user="alice@example",
)

same_runtime = Dml.from_config_vars(
    {
        "project_home": ".",
        "remote.root": "s3://my-bucket/demo",
        "remote.fetch_workers": 8,
        "user": "alice@example",
    }
)
```

CLI:

```bash
dml --project-home . --remote-root s3://my-bucket/demo status
dml config show
dml config set remote.root s3://my-bucket/demo
```

## Config file locations and contents

Global config is read from `config.json` under the resolved config home. The current resolver reads flattened canonical keys such as:

- `user`
- `default.branch_name`
- `remote.fetch_workers`

Project config is read from `.dml/config.json`. The current resolver reads flattened canonical keys such as:

- `remote.project`
- `remote.root`
- `remote.fetch_workers`

## Related pages

- [CLI](cli.md)
- [Errors](errors.md)
