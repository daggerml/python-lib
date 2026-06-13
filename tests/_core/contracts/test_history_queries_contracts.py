from __future__ import annotations

from daggerml._core.head import Head
from tests._core.helpers import commit_literal_dag, make_local_dml


def test_status_reports_attached_head_branch_list_and_live_indexes(tmp_path, monkeypatch) -> None:
    dml = make_local_dml(tmp_path, monkeypatch)
    dml.runtime.create()

    assert dml.status() == {
        "mode": "attached",
        "branch": "main",
        "commit": None,
        "branches": [],
        "num_indexes": 1,
        "ahead": None,
        "behind": None,
    }


def test_status_reports_detached_head_without_changing_branch_list(tmp_path, monkeypatch) -> None:
    dml = make_local_dml(tmp_path, monkeypatch)
    commit = commit_literal_dag(dml, "train", 1)

    status = dml.checkout("HEAD")

    assert status == {
        "mode": "detached",
        "branch": None,
        "commit": commit,
        "branches": ["main"],
        "num_indexes": 0,
        "ahead": None,
        "behind": None,
    }


def test_status_reports_ahead_and_behind_relative_to_fetched_remote_branch(tmp_path, monkeypatch) -> None:
    dml = make_local_dml(tmp_path, monkeypatch, remote_project="dml://acme/demo")
    base = commit_literal_dag(dml, "train", 1, message="base")
    commit_literal_dag(dml, "local", 2, message="local")

    # Simulate the last fetched remote-tracking branch still pointing at base.
    from daggerml._core.head import Head

    head = Head(str(tmp_path))
    head.create_remote_ref("acme", "demo", "main", base)

    status = dml.status()

    assert status["ahead"] == 1
    assert status["behind"] == 0


def test_status_reports_diverged_counts_against_fetched_remote_branch(tmp_path, monkeypatch) -> None:
    dml = make_local_dml(tmp_path, monkeypatch, remote_project="dml://acme/demo")
    base = commit_literal_dag(dml, "train", 1, message="base")

    from daggerml._core.head import Head

    head = Head(str(tmp_path))
    head.create_local_ref("feature", base)
    dml.checkout("feature")
    commit_literal_dag(dml, "feature-only", 2, message="feature-only")
    dml.checkout("main")
    remote_tip = commit_literal_dag(dml, "remote-only", 3, message="remote-only")

    head.create_remote_ref("acme", "demo", "feature", remote_tip)
    dml.checkout("feature")

    status = dml.status()

    assert status["ahead"] == 1
    assert status["behind"] == 1


def test_log_show_and_diff_use_parent_relative_history_by_default(tmp_path, monkeypatch) -> None:
    dml = make_local_dml(tmp_path, monkeypatch)
    commit_literal_dag(dml, "train", 1, message="train-v1")
    second = commit_literal_dag(dml, "eval", 2, message="eval-v1")

    diff = dml.diff(second)
    show = dml.show(second)
    log = dml.log(limit=2)["commits"]

    assert diff == {"added": {"eval": show["dags"]["eval"]}, "removed": {}, "modified": {}}
    assert show["diff"] == diff
    assert show["message"] == "eval-v1"
    assert "dag" not in show
    assert set(show["dags"]) == {"train", "eval"}
    assert [entry["message"] for entry in log] == ["eval-v1", "train-v1"]
    assert all("dag" not in entry for entry in log)


def test_diff_accepts_explicit_base_for_modified_dag_name(tmp_path, monkeypatch) -> None:
    dml = make_local_dml(tmp_path, monkeypatch)
    first = commit_literal_dag(dml, "train", 1, message="train-v1")
    second = commit_literal_dag(dml, "train", 2, message="train-v2")

    diff = dml.diff(second, relative_to=first)

    assert diff == {
        "added": {},
        "removed": {},
        "modified": {"train": (dml.show(first)["dags"]["train"], dml.show(second)["dags"]["train"])},
    }


def test_runtime_commit_without_name_returns_dag_and_leaves_history_unchanged(tmp_path, monkeypatch) -> None:
    dml = make_local_dml(tmp_path, monkeypatch)
    base = dml.status()["commit"]
    index = dml.runtime.create()
    node = dml.runtime.put_literal(index, 1, name="value")

    dag_ref = dml.runtime.commit(index, node, message="unnamed")

    assert dag_ref.ns() == "dag"
    assert dml.status()["commit"] == base
    assert dml.runtime.list() == []


def test_runtime_describe_and_list_include_commit_shaped_payload_plus_dag(tmp_path, monkeypatch) -> None:
    dml = make_local_dml(tmp_path, monkeypatch)
    base = dml.status()["commit"]
    index = dml.runtime.create()

    described = dml.runtime.describe(index)
    listed = dml.runtime.list()

    assert described["id"] == index
    assert described["parents"] == ([] if base is None else [base])
    assert described["author"] == "tester"
    assert described["message"] == ""
    assert described["dag"].ns() == "dag"
    assert described["tree"].ns() == "tree"

    assert listed == [described]


def test_runtime_describe_graph_accepts_explicit_roots(tmp_path, monkeypatch) -> None:
    dml = make_local_dml(tmp_path, monkeypatch)
    index = dml.runtime.create()

    graph = dml.runtime.describe_graph(index)

    assert graph["roots"] == [index.id()]
    assert graph["nodes"][index.id()]["execution_id"] == index.id()
    assert graph["nodes"][index.id()]["spawned"] == []
    assert graph["nodes"][index.id()]["children"] == []


def test_runtime_describe_graph_defaults_to_open_local_indexes(tmp_path, monkeypatch) -> None:
    dml = make_local_dml(tmp_path, monkeypatch)
    first = dml.runtime.create()
    second = dml.runtime.create()

    graph = dml.runtime.describe_graph()

    assert set(graph["roots"]) == {first.id(), second.id()}
    assert set(graph["nodes"]) == {first.id(), second.id()}


def test_first_named_commit_materializes_unborn_branch_ref_and_branch_list(tmp_path, monkeypatch) -> None:
    dml = make_local_dml(tmp_path, monkeypatch)
    head = Head(str(tmp_path))

    commit = commit_literal_dag(dml, "train", 1, message="train-v1")

    assert head.get_local_ref("main") == commit
    assert dml.branch.list() == ["main"]
