from __future__ import annotations

from collections.abc import Callable

import pytest

from daggerml._core.types import DmlRepoError
from tests._core.helpers import commit_literal_dag, make_local_dml


def test_dag_checkout_overwrites_name(tmp_path, monkeypatch) -> None:
    dml = make_local_dml(tmp_path, monkeypatch)
    first = commit_literal_dag(dml, "source", 1)
    commit_literal_dag(dml, "target", 2)
    source_ref = dml.show(first)["dags"]["source"]

    dml.dag.checkout(source_ref, name="target")

    assert dml.show("HEAD")["dags"]["target"] == source_ref


def test_dag_delete_removes_named_dag_and_missing_delete_fails(tmp_path, monkeypatch) -> None:
    dml = make_local_dml(tmp_path, monkeypatch)
    commit_literal_dag(dml, "train", 1)
    commit_literal_dag(dml, "eval", 2)

    dml.dag.delete("train")

    assert set(dml.show("HEAD")["dags"]) == {"eval"}
    with pytest.raises(DmlRepoError, match="DAG 'missing' not found"):
        dml.dag.delete("missing")


def test_dag_checkout_on_unborn_head_materializes_first_branch_commit(tmp_path, monkeypatch) -> None:
    source = make_local_dml(tmp_path / "source", monkeypatch)
    source_commit = commit_literal_dag(source, "source", 1)
    source_ref = source.show(source_commit)["dags"]["source"]
    target = make_local_dml(tmp_path / "target", monkeypatch)
    target.dag.checkout(source_ref, name="source")
    assert target.show("HEAD")["dags"]["source"] == source_ref
    assert target.status()["commit"] is not None
    assert target.branch.list() == ["main"]


@pytest.mark.parametrize(
    ("op", "message"),
    [
        pytest.param(
            lambda dml, dag: dml.dag.checkout(dag, name="copy"),
            "Cannot checkout DAG when HEAD is detached",
            id="REPO-DAG-001:checkout-detached",
        ),
        pytest.param(
            lambda dml, dag: dml.dag.delete("source"),
            "Cannot delete DAG when HEAD is detached",
            id="REPO-DAG-002:delete-detached",
        ),
    ],
)
def test_dag_tree_mutations_require_attached_head(
    tmp_path,
    monkeypatch,
    op: Callable,
    message: str,
) -> None:
    dml = make_local_dml(tmp_path, monkeypatch)
    commit = commit_literal_dag(dml, "source", 1)
    dag_ref = dml.show(commit)["dags"]["source"]
    dml.checkout("HEAD")

    with pytest.raises(DmlRepoError, match=message):
        op(dml, dag_ref)
