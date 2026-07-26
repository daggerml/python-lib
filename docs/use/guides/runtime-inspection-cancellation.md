# Inspect and cancel runtimes

Find open runtimes before diagnosing or stopping active work:

```bash
dml runtime list
dml runtime describe INDEX_REF
dml runtime describe-graph INDEX_REF
```

Cancel a runtime with the reference returned by `list`:

```bash
dml runtime cancel INDEX_REF --mode full
```

Use `--mode drive` when DaggerML should also attempt to stop running tasks. Cancellation is coordinated through remote execution state for remote-backed functions; inspect the graph again afterwards to see lineage and terminal state.
