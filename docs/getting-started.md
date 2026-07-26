# Get started

DaggerML requires Python 3.10 or later. Install it, create a project with the CLI, then author a first DAG in Python.

```bash
pip install daggerml
mkdir research-demo
cd research-demo
dml init
dml status
```

`dml init` creates `.dml/` in the current directory. Create `first_dag.py`:

```python
import daggerml as dml

with dml.new("first-result", message="record a first result") as dag:
    inputs = dag.put({"samples": [2, 3, 5]}, name="inputs")
    dag.commit(inputs)
```

Run it from the initialized project, then inspect the recorded result and history:

```bash
python first_dag.py
dml show
dml log
```

Continue with [DAGs, nodes, and results](use/concepts/dags-nodes-results.md), then [author a DAG](use/guides/author-a-dag.md). Use `dml --help` and [the CLI reference](use/reference/cli.md) for generated command details.
