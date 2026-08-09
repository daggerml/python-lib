# Share and reuse research

Configure a remote root, publish history, and share a branch or tag revision:

```bash
dml config set remote.root s3://bucket/research
dml push
dml tag create paper-v1
dml push @paper-v1
```

Collaborators can fetch a branch or clone it into a new directory:

```bash
dml fetch main
dml clone main --project-home colleague-copy
```

Reuse committed work in Python with `dml.load("dag-name")` or import it into new work with `dag.require("dag-name")`. A normal push fast-forwards branches and creates tags; use `--force` only when intentionally replacing a remote ref.
