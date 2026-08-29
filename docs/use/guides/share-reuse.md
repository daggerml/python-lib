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

When only the current research snapshot is needed, limit commit history without
limiting the snapshot itself:

```bash
dml clone main --depth 1 --project-home colleague-copy
dml fetch --dep models --depth 2 main
```

Later, deepen or complete the selected history:

```bash
dml fetch --depth 10 main
dml fetch --unshallow main
```

Reuse committed work in Python with `dml.load("dag-name")` or import it into new work with `dag.require("dag-name")`. A normal push can advance the branch from a shallow clone when the observed remote tip anchors the omitted history. Creating a new remote branch or forcing publication from shallow history requires unshallowing first.
