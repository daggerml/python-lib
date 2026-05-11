from __future__ import annotations

from daggerml._internal import Dml
from daggerml.api import new


def test_show_log_and_diff_return_spec_shapes():
    with Dml.temporary() as dml:
        with new(dml=dml, name="baseline", message="baseline") as dag:
            result = dag.put(1, name="result")
            dag.commit(result)

        shown = dml.show()
        logged = dml.log()
        diffed = dml.diff("HEAD~1", "HEAD")

    assert set(shown.keys()) == {"revision", "commit", "dags", "change"}
    assert set(logged.keys()) == {"revision", "commits"}
    assert set(diffed.keys()) == {"left", "right", "added", "removed", "updated"}
    assert "baseline" in shown["dags"]


def test_branch_lists_local_and_remote_tracking_views(tmp_path):
    with Dml.temporary(repo="source") as source:
        remote_uri = source._context.remote_uri
        source.push(None, branch="main", create=True, force=False)
        source_uri = source.config.get("project.uri")

        with Dml.temporary(repo="target", remote_root=remote_uri) as target:
            target.fetch(source_uri, None)
            local = target.branch()
            remote = target.branch(remote=True)

    assert local["remote"] is False
    assert "main" in local["branches"]
    assert remote["remote"] is True
    assert any(branch.startswith("dml://") for branch in remote["branches"])
