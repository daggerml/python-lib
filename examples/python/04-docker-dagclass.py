"""Run a self-contained dagclass method in the example Docker image.

Run ``01-docker_dataset.py`` first. This example imports its built image and
Docker flags as dagclass attributes, then compiles both the outer Docker refs
and the method-body ``self.label`` ref into the same dagclass namespace.
"""

from __future__ import annotations

import argparse

import daggerml as dml
from daggerml.contrib import api


@api.dagclass
class DockerSummary:
    image: object
    flags: list[str]
    label: str

    @api.funkify(uri="docker", image=api.ref("image"), flags=api.ref("flags"))
    @api.funkify
    def main(self, values):
        import pandas as pd  # pyright: ignore[reportMissingImports]

        series = pd.Series(values.value())
        return {
            "label": self.label.value(),
            "count": int(series.count()),
            "mean": float(series.mean()),
        }


def main(dag_name: str, docker_dag_name: str) -> None:
    docker_dag = dml.load(docker_dag_name)
    summary = DockerSummary(
        image=docker_dag.image,
        flags=docker_dag["dkr-flags"],
        label="computed inside the example image",
    )

    api.run(summary, [2, 4, 8], name=dag_name)
    print(dml.load(dag_name).result.value())


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("dag_name")
    parser.add_argument("docker_dag_name")
    args = parser.parse_args()
    main(args.dag_name, args.docker_dag_name)
