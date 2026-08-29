"""Run an end-to-end Docker-backed dataset pipeline.

The example builds a Docker image from this repository, loads the iris dataset
in one Docker-executed funk, and trains a small classifier in another. It
exercises the contrib runtime end to end: script funkification, Docker
execution, remote cache publication, and S3-backed artifact exchange between
DAG nodes.
"""

import argparse

import daggerml as dml


def main(dag_name: str, hello_dag_name: str) -> None:
    dag = dml.new(name=dag_name)
    print("Training model and generating predictions within Docker...")
    loaded_dag = dml.load(hello_dag_name)
    dag.old_result = loaded_dag.greeting
    dag.hello_fn = loaded_dag.hello_fn
    # dag.hello_fn = loaded_dag.greeting.context(root=False).argv.value()[1]
    print(dag.hello_fn(42).value())
    print(dag.hello_fn(-1).value())
    dag.commit(dag.hello_fn(42))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("dag_name")
    parser.add_argument("hello_dag_name")
    args = parser.parse_args()
    main(args.dag_name, args.hello_dag_name)
