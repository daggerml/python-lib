from __future__ import annotations

import pytest

from daggerml._core.head import Head
from daggerml._core.types import DmlRepoError
from tests._core.helpers import commit_literal_dag, make_local_dml


def _make_shallow_history(tmp_path, monkeypatch):
    dml = make_local_dml(tmp_path, monkeypatch)
    first = commit_literal_dag(dml, "one", 1)
    second = commit_literal_dag(dml, "two", 2)
    third = commit_literal_dag(dml, "three", 3)
    with dml._db.tx() as txn:
        txn.delete(first)
    Head(str(tmp_path)).write_shallow_commits({first})
    return dml, first, second, third


def test_log_reports_shallow_truncation_and_available_commits(tmp_path, monkeypatch) -> None:
    dml, _first, second, third = _make_shallow_history(tmp_path, monkeypatch)

    result = dml.log(limit=10)

    assert [item["id"] for item in result["commits"]] == [third.id(), second.id()]
    assert result["truncated"] is True


def test_revision_and_implicit_parent_comparison_fail_at_shallow_boundary(tmp_path, monkeypatch) -> None:
    dml, _first, second, _third = _make_shallow_history(tmp_path, monkeypatch)

    with pytest.raises(DmlRepoError, match="fetch with greater depth or --unshallow"):
        dml.rev_parse("HEAD~2")
    with pytest.raises(DmlRepoError, match="fetch with greater depth or --unshallow"):
        dml.show(second)


def test_explicit_diff_of_available_snapshots_works_with_shallow_history(tmp_path, monkeypatch) -> None:
    dml, _first, second, third = _make_shallow_history(tmp_path, monkeypatch)

    result = dml.diff(third, second)

    assert set(result["added"]) == {"three"}


def test_fast_forward_proven_above_shallow_boundary_succeeds(tmp_path, monkeypatch) -> None:
    dml, _first, second, third = _make_shallow_history(tmp_path, monkeypatch)
    head = Head(str(tmp_path))
    head.update_local_ref("main", second)
    head.update_remote_tracking_ref("main", third)

    result = dml.merge("main", remote=True)

    assert result["commit"] == third


def test_status_reports_equal_tips_but_not_incomplete_counts(tmp_path, monkeypatch) -> None:
    dml, _first, second, third = _make_shallow_history(tmp_path, monkeypatch)
    head = Head(str(tmp_path))
    head.set_upstream("main", "main")
    head.update_remote_tracking_ref("main", third)

    assert dml.status()["ahead"] == 0
    assert dml.status()["behind"] == 0

    head.update_local_ref("main", second)
    assert dml.status()["ahead"] is None
    assert dml.status()["behind"] is None
