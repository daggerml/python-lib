"""Run an end-to-end Docker-backed dataset pipeline.

The example builds a Docker image from this repository, loads the iris dataset
in one Docker-executed funk, and trains a small classifier in another. It
exercises the contrib runtime end to end: script funkification, Docker
execution, remote cache publication, and S3-backed artifact exchange between
DAG nodes.
"""

import argparse
import json

import daggerml as dml


def main(dag_name: str, docker_dag_name: str) -> None:
    dag = dml.new(name=dag_name)
    print("Training model and generating predictions within Docker...")
    loaded_dag = dml.load(docker_dag_name)
    dag.predict_fn = loaded_dag.predict_fn
    dag.dataset = loaded_dag.dataset
    predictions = dag.predict_fn(
        dag.dataset,
        {"max_iter": 200, "solver": "saga", "penalty": "elasticnet", "l1_ratio": 0.5},
        name="predictions",
    )
    print("Committing DAG to persist artifacts...")
    dag.commit(predictions)
    print("Reading predictions parquet from S3...")
    print(json.dumps(predictions.value(), indent=2))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("dag_name")
    parser.add_argument("docker_dag_name")
    args = parser.parse_args()
    main(args.dag_name, args.docker_dag_name)
