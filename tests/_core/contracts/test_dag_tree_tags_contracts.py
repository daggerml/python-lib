from __future__ import annotations

import pytest

from daggerml._core.commit import CommitOps
from daggerml._core.db import Ref
from daggerml._core.head import Head
from daggerml._core.types import DmlRepoError, Tree
from tests._core.helpers import commit_literal_dag, make_local_dml


def test_tree_tags_require_known_dag_names_and_string_lists() -> None:
    Tree(dags={"trial": Ref("dag:trial")}, tags={"trial": ["research.v0"]})._validate()

    with pytest.raises(TypeError, match="requires a named DAG"):
        Tree(dags={}, tags={"missing": ["research.v0"]})._validate()
    with pytest.raises(TypeError, match="must be a list of strings"):
        Tree(dags={"trial": Ref("dag:trial")}, tags={"trial": "research.v0"})._validate()  # type: ignore[arg-type]
    with pytest.raises(TypeError, match="must be a list of strings"):
        Tree(dags={"trial": Ref("dag:trial")}, tags={"trial": [1]})._validate()  # type: ignore[list-item]


def test_tree_from_dict_rejects_payload_without_required_tags() -> None:
    with pytest.raises(TypeError, match="missing 1 required positional argument: 'tags'"):
        Tree.from_dict({"dags": {}})


def test_dag_tag_add_remove_and_history_inspection(tmp_path, monkeypatch) -> None:
    dml = make_local_dml(tmp_path, monkeypatch)
    base = commit_literal_dag(dml, "trial", 1)

    tagged = dml.dag.add_tag("trial", "research.v0")

    assert tagged != base
    assert dml.show(base)["tags"] == {}
    assert dml.show(tagged)["tags"] == {"trial": ["research.v0"]}
    assert dml.log(limit=2)["commits"][1]["tags"] == {}
    assert dml.dag.add_tag("trial", "research.v0") == tagged

    untagged = dml.dag.remove_tag("trial", "research.v0")

    assert untagged != tagged
    assert dml.show(untagged)["tags"] == {}
    assert dml.dag.remove_tag("trial", "research.v0") == untagged


@pytest.mark.parametrize("method", ["add_tag", "remove_tag"])
def test_dag_tag_mutation_rejects_missing_dag_and_detached_head(tmp_path, monkeypatch, method: str) -> None:
    dml = make_local_dml(tmp_path, monkeypatch)
    commit_literal_dag(dml, "trial", 1)

    with pytest.raises(DmlRepoError, match="DAG 'missing' not found"):
        getattr(dml.dag, method)("missing", "research.v0")

    dml.checkout("HEAD")
    with pytest.raises(DmlRepoError, match="Cannot .* DAG tag when HEAD is detached"):
        getattr(dml.dag, method)("trial", "research.v0")


def test_concurrent_tag_edits_conflict_during_merge_and_rebase(tmp_path, monkeypatch) -> None:
    dml = make_local_dml(tmp_path, monkeypatch)
    base = commit_literal_dag(dml, "trial", 1)
    Head(str(tmp_path)).create_local_ref("feature", base)

    dml.checkout("feature")
    feature_tagged = dml.dag.add_tag("trial", "research.v0")
    dml.checkout("main")
    main_tagged = dml.dag.add_tag("trial", "baseline")

    with pytest.raises(DmlRepoError, match=r"Merge conflicts: \['trial'\]"):
        CommitOps().merge(main_tagged, feature_tagged, user="tester", db=dml._db)

    dml.checkout("feature")
    with pytest.raises(DmlRepoError, match=r"Rebase conflicts: \['trial'\]"):
        dml.rebase("main")


def test_tag_history_operations_preserve_unchanged_entries(tmp_path, monkeypatch) -> None:
    dml = make_local_dml(tmp_path, monkeypatch)
    commit_literal_dag(dml, "trial", 1)
    dml.dag.add_tag("trial", "research.v0")
    tagged = dml.status()["commit"]
    assert tagged is not None
    Head(str(tmp_path)).create_local_ref("feature", tagged)

    dml.checkout("feature")
    commit_literal_dag(dml, "feature-only", 2)
    feature_tip = dml.status()["commit"]
    assert feature_tip is not None
    dml.checkout("main")
    main_tip = commit_literal_dag(dml, "main-only", 3)

    merged = CommitOps().merge(main_tip, feature_tip, user="tester", db=dml._db)
    assert CommitOps().show(merged, db=dml._db)["tags"] == {"trial": ["research.v0"]}

    dml.checkout("feature")
    dml.rebase("main")
    assert dml.show("HEAD")["tags"] == {"trial": ["research.v0"]}


def test_tag_revert_checkout_replacement_and_delete_clear_tags(tmp_path, monkeypatch) -> None:
    dml = make_local_dml(tmp_path, monkeypatch)
    source_commit = commit_literal_dag(dml, "source", 1)
    commit_literal_dag(dml, "target", 2)
    tagged = dml.dag.add_tag("target", "research.v0")

    dml.revert("HEAD")
    assert dml.show("HEAD")["tags"] == {}

    dml.dag.add_tag("target", "research.v0")
    dml.dag.checkout(source_commit, "source", name="target", replace=True)
    assert dml.show("HEAD")["tags"] == {}

    dml.dag.add_tag("target", "research.v0")
    dml.dag.delete("target")
    assert dml.show("HEAD")["tags"] == {}
    assert tagged != dml.status()["commit"]


def test_revert_unrelated_change_preserves_tags(tmp_path, monkeypatch) -> None:
    dml = make_local_dml(tmp_path, monkeypatch)
    commit_literal_dag(dml, "trial", 1)
    dml.dag.add_tag("trial", "research.v0")
    commit_literal_dag(dml, "other", 2)

    dml.revert("HEAD")

    assert dml.show("HEAD")["tags"] == {"trial": ["research.v0"]}
