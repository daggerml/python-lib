"""Record a small multi-file analysis report in a DaggerML DAG."""

from analysis.metrics import summarize

import daggerml.api as api


def run():
    dag = api.new("docs-analysis-report")
    report = dag.put(summarize([3, 5, 8]), name="report")
    dag.commit(report)
    assert report.value() == {"count": 3, "total": 16, "largest": 8}


if __name__ == "__main__":
    run()
