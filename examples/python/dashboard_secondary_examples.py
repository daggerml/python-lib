"""Create distinct literal DAGs for the disposable dashboard demo project."""

from __future__ import annotations

from daggerml import Dml


def main() -> None:
    dml = Dml()
    examples = [
        (
            "examples/model-evaluation",
            "Add model evaluation metrics",
            "metrics",
            {"accuracy": 0.94, "loss": 0.18},
        ),
        (
            "examples/data-preparation",
            "Record data preparation artifacts",
            "artifacts",
            ["raw.csv", "clean.parquet", "features.parquet"],
        ),
        (
            "examples/benchmark-report",
            "Capture benchmark report",
            "benchmark",
            {"p50_ms": 42, "p95_ms": 88, "requests": 12_000},
        ),
        (
            "examples/release-gate",
            "Store release decision",
            "decision",
            {"threshold": 0.72, "approved": True},
        ),
    ]
    for name, message, node_name, value in examples:
        index = dml.runtime.create()
        node = dml.runtime.put_literal(index, value, name=node_name)
        dml.runtime.commit(index, node, name=name, message=message)


if __name__ == "__main__":
    main()
