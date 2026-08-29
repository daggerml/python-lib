from __future__ import annotations

from collections.abc import Callable

import pytest

from daggerml._core.commit import CommitOps
from daggerml._core.head import Head
from daggerml._core.types import DmlRepoError
from tests._core.helpers import commit_literal_dag, make_local_dml


def test_merge_fast_forwards_current_branch_to_merged_revision(tmp_path, monkeypatch) -> None:
    dml = make_local_dml(tmp_path, monkeypatch)
    base = commit_literal_dag(dml, "train", 1)
    Head(str(tmp_path)).create_local_ref("feature", base)
    dml.checkout("feature")
    feature_tip = commit_literal_dag(dml, "eval", 2, message="feature")
    dml.checkout("main")

    status = dml.merge("feature")

    assert status["branch"] == "main"
    assert status["commit"] == feature_tip
    assert set(dml.show("HEAD")["dags"]) == {"train", "eval"}


def test_merge_advances_unborn_attached_head_without_synthetic_base(tmp_path, monkeypatch) -> None:
    dml = make_local_dml(tmp_path, monkeypatch)
    source = commit_literal_dag(dml, "train", 1)
    Head(str(tmp_path)).create_local_ref("feature", source)
    dml.checkout("main")

    status = dml.merge("feature")

    assert status["branch"] == "main"
    assert status["commit"] == source
    assert Head(str(tmp_path)).get_local_ref("main") == source


def test_rebase_replays_linear_history_onto_target_branch(tmp_path, monkeypatch) -> None:
    dml = make_local_dml(tmp_path, monkeypatch)
    base = commit_literal_dag(dml, "train", 1)
    Head(str(tmp_path)).create_local_ref("feature", base)
    dml.checkout("feature")
    feature_tip = commit_literal_dag(dml, "model", 2, message="feature")
    dml.checkout("main")
    commit_literal_dag(dml, "eval", 3, message="main")
    dml.checkout("feature")

    status = dml.rebase("main")

    assert status["branch"] == "feature"
    assert status["commit"] != feature_tip
    assert dml.log(limit=1)["commits"][0]["message"] == "feature"
    assert set(dml.show("HEAD")["dags"]) == {"train", "model", "eval"}


def test_revert_creates_inverse_commit_on_current_branch(tmp_path, monkeypatch) -> None:
    dml = make_local_dml(tmp_path, monkeypatch)
    commit_literal_dag(dml, "train", 1, message="train-v1")
    commit_literal_dag(dml, "eval", 2, message="eval-v1")

    status = dml.revert("HEAD", message="undo eval")

    assert status["branch"] == "main"
    assert dml.log(limit=1)["commits"][0]["message"] == "undo eval"
    assert set(dml.show("HEAD")["dags"]) == {"train"}


@pytest.mark.parametrize(
    ("op", "revision", "message"),
    [
        pytest.param(
            lambda dml, revision: dml.merge(revision),
            "main",
            "Cannot merge when HEAD is detached",
            id="REPO-DET-001:merge",
        ),
        pytest.param(
            lambda dml, revision: dml.rebase(revision),
            "main",
            "Cannot rebase when HEAD is detached",
            id="REPO-DET-002:rebase",
        ),
        pytest.param(
            lambda dml, revision: dml.revert(revision),
            "HEAD",
            "Cannot revert when HEAD is detached",
            id="REPO-DET-003:revert",
        ),
    ],
)
def test_attached_history_mutations_reject_detached_head(
    tmp_path,
    monkeypatch,
    op: Callable,
    revision: str,
    message: str,
) -> None:
    dml = make_local_dml(tmp_path, monkeypatch)
    commit_literal_dag(dml, "train", 1)
    dml.checkout("HEAD")

    with pytest.raises(DmlRepoError, match=message):
        op(dml, revision)


def test_commit_ops_merge_detects_conflicting_dag_names(tmp_path, monkeypatch) -> None:
    dml = make_local_dml(tmp_path, monkeypatch)
    base = commit_literal_dag(dml, "train", 1)
    Head(str(tmp_path)).create_local_ref("feature", base)
    dml.checkout("feature")
    feature_tip = commit_literal_dag(dml, "train", 2)
    dml.checkout("main")
    main_tip = commit_literal_dag(dml, "train", 3)

    with pytest.raises(DmlRepoError, match=r"Merge conflicts: \['train'\]"):
        CommitOps().merge(main_tip, feature_tip, user="tester", ff_only=False, db=dml._db)


def test_commit_ops_revert_detects_conflicts_against_current_tree(tmp_path, monkeypatch) -> None:
    dml = make_local_dml(tmp_path, monkeypatch)
    commit_literal_dag(dml, "train", 1)
    changed = commit_literal_dag(dml, "train", 2)
    current = commit_literal_dag(dml, "train", 3)

    with pytest.raises(DmlRepoError, match=r"Revert conflicts: \['train'\]"):
        CommitOps().revert(changed, current, user="tester", db=dml._db)


def test_commit_ops_rebase_replays_linear_commits_and_preserves_message_order(tmp_path, monkeypatch) -> None:
    dml = make_local_dml(tmp_path, monkeypatch)
    base = commit_literal_dag(dml, "train", 1)
    Head(str(tmp_path)).create_local_ref("feature", base)
    dml.checkout("feature")
    commit_literal_dag(dml, "model", 2, message="model")
    feature_tip = commit_literal_dag(dml, "eval", 3, message="eval")
    dml.checkout("main")
    main_tip = commit_literal_dag(dml, "main-only", 4, message="main-only")

    rebased = CommitOps().rebase(feature_tip, main_tip, user="tester", db=dml._db)

    assert set(CommitOps().show(rebased, db=dml._db)["dags"]) == {"train", "model", "eval", "main-only"}
    assert [entry["message"] for entry in CommitOps().log(rebased, limit=2, db=dml._db)] == ["eval", "model"]


def test_commit_ops_get_ancestor_uses_first_parent_for_merge_commits(tmp_path, monkeypatch) -> None:
    dml = make_local_dml(tmp_path, monkeypatch)
    base = commit_literal_dag(dml, "train", 1)
    Head(str(tmp_path)).create_local_ref("feature", base)
    dml.checkout("feature")
    feature_tip = commit_literal_dag(dml, "eval", 2)
    dml.checkout("main")
    main_tip = commit_literal_dag(dml, "model", 3)

    merged = CommitOps().merge(main_tip, feature_tip, user="tester", ff_only=False, db=dml._db)

    assert CommitOps().get_ancestor(merged, 1, db=dml._db) == main_tip
    assert CommitOps().get_ancestor(merged, 2, db=dml._db) == base
