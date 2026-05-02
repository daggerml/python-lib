"""Run an end-to-end Docker-backed dataset pipeline.

The example builds a Docker image from this repository, loads the iris dataset
in one Docker-executed funk, and trains a small classifier in another. It
exercises the contrib runtime end to end: script funkification, Docker
execution, remote cache publication, and S3-backed artifact exchange between
DAG nodes.
"""

import daggerml as dml


def main() -> None:
    with dml.new("examples/01b-load-fn") as dag:
        print("Training model and generating predictions within Docker...")
        loaded_dag = dml.load("examples/00-hello-world")
        dag.old_result = loaded_dag.greeting
        dag.hello_fn = loaded_dag.greeting.load().argv.value()[0]
        print(dag.hello_fn(42).value())
        print(dag.hello_fn(-1).value())


if __name__ == "__main__":
    main()
