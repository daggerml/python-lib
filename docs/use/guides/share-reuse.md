# Share and reuse research

Configure a project remote, publish history, and share a branch or tag revision:

```bash
dml config set remote.root s3://bucket/research
dml config set remote.project dml://alice/research
dml push
dml tag create paper-v1
dml push @paper-v1
```

Collaborators can fetch a project ref or clone it into a new directory:

```bash
dml fetch dml://alice/research#main
dml clone dml://alice/research#main --project-home colleague-copy
```

Reuse committed work in Python with `dml.load("dag-name")` or import it into new work with `dag.require("dag-name")`. A normal push fast-forwards branches and creates tags; use `--force` only when intentionally replacing a remote ref.
