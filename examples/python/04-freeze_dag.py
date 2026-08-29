"""Create a partial DAG and freeze it for read-only inspection.

Run this example in an initialized DaggerML project. The resulting DAG remains
uncommitted, but its named nodes are available through the frozen runtime.
"""

from __future__ import annotations

import argparse

import daggerml as dml


def main(dag_name: str) -> None:
    """Stage several nodes, then freeze the unfinished DAG with an annotation."""
    dag = dml.new(name=dag_name)
    dag.put(42, name="answer")
    dag.put([2, 3, 5], name="primes")
    dag.put({"source": "example", "status": "ready for review"}, name="metadata")

    frozen = dag.freeze("Pause here to inspect the staged inputs before committing.")
    print(f"Frozen {dag_name!r} with named nodes: {', '.join(frozen.keys())}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("dag_name")
    main(parser.parse_args().dag_name)
