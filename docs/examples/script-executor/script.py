"""A file-backed local-script executor example for the documentation build."""

import daggerml.api as api
from daggerml import Dml
from daggerml.contrib.api import funkify


@funkify(adapter="local", uri="script")
def add_one(dag, value):
    return dag.put(value.value() + 1, name="result")


def run():
    runtime = Dml()
    dag = api.new("docs-script-executor", dml=runtime)
    result = dag.put(add_one, name="add-one")(41, name="result", sleep=lambda: 0, timeout=10_000)
    dag.commit(result)
    assert result.value() == 42


if __name__ == "__main__":
    run()
