# Work with remotes

DaggerML splits remote configuration into two pieces:

- `remote.root`: where remote-backed data lives, such as `s3://bucket/prefix`
- `remote.project`: which project name to sync, such as `dml://alice/demo`

You can have `remote.root` without `remote.project`, but project sync commands need both.

## Initialize a repo with remote sync enabled

```bash
dml init \
  --project-home ./demo-repo \
  --user alice@example.com \
  --remote-root s3://bucket/prefix \
  --remote-project dml://alice/demo
```

If you are adding remote sync to an existing repo, you can update config directly:

```bash
dml --project-home ./demo-repo config set remote.root s3://bucket/prefix
dml --project-home ./demo-repo config set remote.project dml://alice/demo
```

## Check what remote config is active

```bash
dml --project-home ./demo-repo config show
```

Python equivalent:

```python
from daggerml import Dml

dml = Dml(project_home="./demo-repo", remote_root="s3://bucket/prefix")

print(dml.config.show())
```

## Discover remote projects and refs

```bash
dml --project-home ./demo-repo admin remote list --owner alice
dml --project-home ./demo-repo admin remote list --project dml://alice/demo
```

Use the first form to discover projects for one owner and the second to list that project's tracked branches and tags.

## Sync history

```bash
dml --project-home ./demo-repo fetch dml://alice/demo#main
dml --project-home ./demo-repo pull
dml --project-home ./demo-repo push

# Intentionally replace a remote branch or tag ref.
dml --project-home ./demo-repo push --revision @v1 --force
```

Python equivalents:

```python
from daggerml import Dml

dml = Dml(
    project_home="./demo-repo",
    remote_root="s3://bucket/prefix",
    remote_project="dml://alice/demo",
    user="alice@example.com",
)

dml.fetch("dml://alice/demo#main")
dml.pull()
dml.push()

# A normal tag push is create-only; force explicitly replaces it.
dml.push("@v1", force=True)
```

## When to use each command

- `fetch` updates local history from a remote branch without changing your current branch.
- `pull` fetches a remote branch and merges it into a local branch.
- `push` publishes a local branch or tag to the configured remote project.

## Related docs

- [Remotes](../concepts/remotes.md)
- [CLI](../reference/cli.md)
- [Configuration](../reference/configuration.md)
- [Errors](../reference/errors.md)
