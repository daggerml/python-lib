from __future__ import annotations

from daggerml._core.head import Head
from tests._core.helpers import commit_literal_dag, make_local_dml


def test_rev_parse_reports_head_commit_branch_tag_and_remote_refs(tmp_path, monkeypatch) -> None:
    dml = make_local_dml(tmp_path, monkeypatch)
    first = commit_literal_dag(dml, "train", 1, message="train-v1")
    head = Head(str(tmp_path))
    head.create_local_ref("release", first, kind="tag")
    head.create_remote_tracking_ref("main", first)

    head_payload = dml.rev_parse("HEAD")
    assert head_payload["kind"] == "head"
    assert head_payload["commit"] == first
    assert head_payload["uri"] is None

    branch_payload = dml.rev_parse("main")
    assert branch_payload["kind"] == "ref"
    assert branch_payload["branch"] == "main"
    assert branch_payload["tag"] is None
    assert branch_payload["uri"] is None
    assert branch_payload["commit"] == first

    tag_payload = dml.rev_parse("@release")
    assert tag_payload["kind"] == "ref"
    assert tag_payload["branch"] is None
    assert tag_payload["tag"] == "release"
    assert tag_payload["uri"] is None
    assert tag_payload["commit"] == first

    commit_payload = dml.rev_parse(first.id())
    assert commit_payload["kind"] == "commit"
    assert commit_payload["uri"] is None
    assert commit_payload["commit"] == first

    remote_payload = dml.rev_parse("main", remote=True)
    assert remote_payload["kind"] == "ref"
    assert remote_payload["branch"] == "main"
    assert remote_payload["tag"] is None
    assert remote_payload["uri"] is None
    assert remote_payload["commit"] == first


def test_rev_parse_resolves_head_ancestry_from_current_head(tmp_path, monkeypatch) -> None:
    dml = make_local_dml(tmp_path, monkeypatch)
    first = commit_literal_dag(dml, "train", 1, message="train-v1")
    second = commit_literal_dag(dml, "eval", 2, message="eval-v1")

    payload = dml.rev_parse("HEAD~1")

    assert payload["kind"] == "head"
    assert payload["commit"] == first
    assert payload["input"] == "HEAD~1"
    assert dml.status()["commit"] == second


def test_rev_parse_allows_unresolved_head_selectors_on_unborn_branch(tmp_path, monkeypatch) -> None:
    dml = make_local_dml(tmp_path, monkeypatch)

    assert dml.rev_parse("HEAD") == {
        "input": "HEAD",
        "uri": None,
        "kind": "head",
        "commit": None,
        "branch": None,
        "tag": None,
    }
    assert dml.rev_parse("HEAD~1")["commit"] is None
