from __future__ import annotations

import pytest

from daggerml._core.db import Ref
from daggerml._core.types import Dag, DmlRepoError, Tree
from tests._core.helpers import make_local_dml


def test_dag_tags_require_sorted_unique_string_lists() -> None:
    Dag(nodes=[], names={}, tags=[])._validate()
    Dag(nodes=[], names={}, tags=["candidate", "research.v0"])._validate()

    with pytest.raises(TypeError, match="must be a list of strings"):
        Dag(nodes=[], names={}, tags=[1])._validate()  # type: ignore[list-item]
    with pytest.raises(TypeError, match="unique and sorted"):
        Dag(nodes=[], names={}, tags=["research.v0", "candidate"])._validate()
    with pytest.raises(TypeError, match="unique and sorted"):
        Dag(nodes=[], names={}, tags=["candidate", "candidate"])._validate()


def test_tree_has_no_tag_storage() -> None:
    Tree(dags={})._validate()
    with pytest.raises(TypeError, match="unexpected keyword argument 'tags'"):
        Tree(dags={"trial": Ref("dag:trial")}, tags={})  # type: ignore[call-arg]


def test_runtime_tags_are_normalized_persisted_and_not_shown_on_commit(tmp_path, monkeypatch) -> None:
    dml = make_local_dml(tmp_path, monkeypatch)
    index = dml.runtime.create(tags=["research.v0", "candidate", "candidate"])

    dml.runtime.add_tag(index, "baseline")
    dml.runtime.remove_tag(index, "research.v0")
    dml.runtime.remove_tag(index, "missing")

    partial = dml.runtime.describe(index)["dag"]
    assert dml.dag.describe(partial)["tags"] == ["baseline", "candidate"]
    node = dml.runtime.put_literal(index, 1)
    dag_ref = dml.runtime.commit(index, node, name="trial")

    assert dml.dag.describe(dag_ref)["tags"] == ["baseline", "candidate"]
    assert "tags" not in dml.show("HEAD")


def test_runtime_tag_mutation_rejects_frozen_index(tmp_path, monkeypatch) -> None:
    dml = make_local_dml(tmp_path, monkeypatch)
    index = dml.runtime.create(tags=["candidate"])
    frozen = dml.runtime.freeze(index)

    with pytest.raises(DmlRepoError):
        dml.runtime.add_tag(frozen, "research.v0")
