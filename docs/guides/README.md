# Guides

Use this section for task-oriented walkthroughs built around the current DaggerML CLI and Python API.

These pages stay focused on the steps. For deeper background, follow the links into [concepts](../concepts/README.md) and [reference](../reference/README.md).

The command examples use `dml` directly. If you are running from a repository checkout instead of an installed CLI, prefix those commands with `uv run`.

## Workflows

- [Create and run a DAG](create-and-run-a-dag.md): initialize a repo, build a DAG in Python, and inspect the result from the CLI.
- [Inspect a repository](inspect-a-repository.md): check HEAD, branches, DAGs, commits, and revision-to-revision changes.
- [Work with remotes](work-with-remotes.md): configure `remote.root` and `remote.project`, inspect remote refs, and sync with `fetch`, `pull`, and `push`.
- [Store and load external data](store-and-load-external-data.md): keep large bytes in S3 while committing `Uri` references into DAG state.
- [Troubleshoot common errors](troubleshoot-common-errors.md): fix the most common setup, sync, and DAG-authoring failures.
