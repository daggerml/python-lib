from __future__ import annotations

import json
from tempfile import TemporaryDirectory

import pytest
from hypothesis import given, settings

from daggerml._core.db import Ref
from daggerml._core.head import Head, _validate_ref_name, _validate_segment
from daggerml._core.types import DmlRepoError
from tests._core.strategies import nested_ref_names


@given(nested_ref_names)
@settings(max_examples=35, deadline=None)
def test_valid_ref_names_round_trip_through_path(name: str) -> None:
    with TemporaryDirectory() as root:
        head = Head(root)
        commit = Ref("commit:" + "a" * 64)

        head.create_local_ref(name, commit)

        assert head.get_local_ref(name) == commit
        assert head.list_local_refs() == [name]
        assert "/" not in head.local_ref_path(name, kind="branch").name


@given(nested_ref_names)
@settings(max_examples=35, deadline=None)
def test_remote_tracking_refs_round_trip(name: str) -> None:
    with TemporaryDirectory() as root:
        head = Head(root)
        commit = Ref("commit:" + "b" * 64)

        head.create_remote_tracking_ref(name, commit, kind="tag")

        assert head.get_remote_tracking_ref(name, kind="tag") == commit
        assert head.list_remote_tracking_refs(kind="tag") == [name]
        path = head.remote_tracking_ref_path(name, kind="tag")
        assert path.parts[-3:] == ("remote", "tags", name.replace("/", "%2F"))


@pytest.mark.parametrize(
    ("value", "message"),
    [
        ("", "expected a non-empty string"),
        (".", "reserved path segment"),
        ("..", "reserved path segment"),
        ("main//x", "empty or reserved path segment"),
        ("main/../x", "empty or reserved path segment"),
        ("UPPER", "must start with a lowercase letter or digit"),
        ("main\\x", "contains"),
    ],
)
def test_invalid_ref_names_describe_the_violation(value: str, message: str) -> None:
    with pytest.raises(ValueError, match=message):
        _validate_ref_name("branch", value)


@pytest.mark.parametrize(
    ("value", "message"),
    [
        ("", "expected a non-empty string"),
        ("owner/project", "single segment"),
        ("UPPER", "must start with a lowercase letter or digit"),
        ("-bad", "must start with a lowercase letter or digit"),
        ("bad space", "contains invalid characters"),
    ],
)
def test_invalid_project_segments_describe_the_violation(value: str, message: str) -> None:
    with pytest.raises(ValueError, match=message):
        _validate_segment("owner", value)


def test_head_attached_detached_and_duplicate_contracts(tmp_path) -> None:
    head = Head(str(tmp_path))
    first = Ref("commit:" + "1" * 64)
    second = Ref("commit:" + "2" * 64)

    head.init(first, "main")
    assert head.get_head() == {"mode": "attached", "branch": "main", "commit": first}
    with pytest.raises(DmlRepoError, match="Branch already exists"):
        head.create_local_ref("main", second)

    head.write_detached_head(second)
    assert head.get_head() == {"mode": "detached", "branch": None, "commit": second}


def test_head_supports_unborn_attached_branch_and_rejects_detached_without_commit(tmp_path) -> None:
    head = Head(str(tmp_path))

    head.init(None, "main")

    assert head.get_head() == {"mode": "attached", "branch": "main", "commit": None}
    with pytest.raises(DmlRepoError, match="Cannot initialize detached HEAD without a commit"):
        head.init(None, "main", detached=True)


def test_local_tag_refs_round_trip_update_and_delete(tmp_path) -> None:
    head = Head(str(tmp_path))
    first = Ref("commit:" + "3" * 64)
    second = Ref("commit:" + "4" * 64)

    head.create_local_ref("release", first, kind="tag")
    assert head.get_local_ref("release", kind="tag") == first
    assert head.list_local_refs(kind="tag") == ["release"]

    head.update_local_ref("release", second, kind="tag")
    assert head.get_local_ref("release", kind="tag") == second

    head.delete_local_ref("release", kind="tag")
    assert head.list_local_refs(kind="tag") == []


def test_remote_tracking_branch_refs_round_trip_update_and_duplicate_rejection(tmp_path) -> None:
    head = Head(str(tmp_path))
    first = Ref("commit:" + "5" * 64)
    second = Ref("commit:" + "6" * 64)

    head.create_remote_tracking_ref("main", first)
    assert head.get_remote_tracking_ref("main") == first
    assert head.list_remote_tracking_refs() == ["main"]

    with pytest.raises(DmlRepoError, match="Branch already exists"):
        head.create_remote_tracking_ref("main", second)

    head.update_remote_tracking_ref("main", second)
    assert head.get_remote_tracking_ref("main") == second

    head.delete_remote_tracking_ref("main")
    assert head.list_remote_tracking_refs() == []


def test_branch_upstream_lifecycle_follows_branch_rename_and_delete(tmp_path) -> None:
    head = Head(str(tmp_path))
    commit = Ref("commit:" + "7" * 64)
    head.create_local_ref("feature", commit)

    assert head.get_upstream("feature") is None
    assert head.set_upstream("feature", "main") == {"branch": "main"}
    head.rename_local_ref("feature", "review")
    assert head.get_upstream("feature") is None
    assert head.get_upstream("review") == {"branch": "main"}

    head.delete_local_ref("review")
    assert head.get_upstream("review") is None


def test_dependency_refs_and_config_are_isolated_gc_roots(tmp_path) -> None:
    head = Head(str(tmp_path))
    branch = Ref("commit:" + "8" * 64)
    tag = Ref("commit:" + "9" * 64)
    head.update_remote_tracking_ref("main", branch)
    head.add_dependency("models", "s3://bucket/models/")
    head.update_dependency_ref("models", "v1", tag, kind="tag")

    assert head.get_dependency_config("models") == {"backend": "s3", "root": "s3://bucket/models"}
    assert head.dependency_config_path("models").parts[-2:] == ("models", "config.json")
    assert set(head.iter_all_remote_tracking_refs()) == {branch, tag}

    head.delete_dependency("models")
    assert head.list_dependencies() == []
    assert set(head.iter_all_remote_tracking_refs()) == {branch}


def test_ref_tip_readers_preserve_exact_tips_and_name_order(tmp_path) -> None:
    head = Head(str(tmp_path))
    local_tip = Ref("commit:" + "a" * 64)
    fetched_tip = Ref("commit:" + "b" * 64)
    dependency_tip = Ref("commit:" + "c" * 64)

    head.create_local_ref("zeta", local_tip)
    head.create_local_ref("alpha", fetched_tip)
    head.update_remote_tracking_ref("zeta", fetched_tip)
    head.update_remote_tracking_ref("alpha", local_tip)
    head.add_dependency("models", "s3://bucket/models")
    head.update_dependency_ref("models", "zeta", dependency_tip)
    head.update_dependency_ref("models", "alpha", fetched_tip)

    assert head.list_local_ref_tips() == [("alpha", fetched_tip), ("zeta", local_tip)]
    assert head.list_remote_tracking_ref_tips() == [("alpha", local_tip), ("zeta", fetched_tip)]
    assert head.list_dependency_ref_tips("models") == [("alpha", fetched_tip), ("zeta", dependency_tip)]


def test_ref_tip_readers_fail_closed_for_malformed_commit_pointer(tmp_path) -> None:
    head = Head(str(tmp_path))
    valid_tip = Ref("commit:" + "a" * 64)
    head.create_local_ref("alpha", valid_tip)
    malformed = head.local_ref_path("zeta", kind="branch")
    malformed.parent.mkdir(parents=True, exist_ok=True)
    malformed.write_text("not-a-commit-id", encoding="utf-8")

    with pytest.raises(DmlRepoError, match="Invalid commit pointer"):
        head.list_local_ref_tips()


@pytest.mark.parametrize(
    "payload",
    [
        {},
        {"backend": "s3", "root": "s3://bucket/models", "unknown": True},
        {"backend": "file", "root": "s3://bucket/models"},
    ],
)
def test_dependency_config_is_strict(tmp_path, payload) -> None:
    head = Head(str(tmp_path))
    path = head.dependency_config_path("models")
    path.parent.mkdir(parents=True)
    path.write_text(json.dumps(payload), encoding="utf-8")

    with pytest.raises(DmlRepoError, match="Invalid dependency config"):
        head.get_dependency_config("models")


def test_shallow_commit_metadata_round_trips_atomically(tmp_path) -> None:
    head = Head(str(tmp_path))
    commits = {Ref("commit:" + "b" * 64), Ref("commit:" + "a" * 64)}

    assert head.get_shallow_commits() == set()
    head.write_shallow_commits(commits)

    assert head.get_shallow_commits() == commits
    assert json.loads(head.shallow_path().read_text(encoding="utf-8")) == {
        "version": 0,
        "missing": ["commit:" + "a" * 64, "commit:" + "b" * 64],
    }

    head.write_shallow_commits(set())
    assert head.get_shallow_commits() == set()
    assert not head.shallow_path().exists()


@pytest.mark.parametrize(
    "payload",
    [
        {},
        {"version": 0},
        {"missing": []},
        {"version": 0, "missing": [], "extra": True},
        {"version": True, "missing": []},
        {"version": False, "missing": []},
        {"version": 0.0, "missing": []},
        {"version": "0", "missing": []},
        {"version": 1, "missing": []},
        {"version": 2, "missing": []},
        {"version": 0, "missing": "commit:" + "a" * 64},
        {"version": 0, "missing": ["dag:" + "a" * 64]},
        {"version": 0, "missing": ["commit:short"]},
        {"version": 0, "missing": ["commit:" + "b" * 64, "commit:" + "a" * 64]},
        {"version": 0, "missing": ["commit:" + "a" * 64, "commit:" + "a" * 64]},
    ],
)
def test_shallow_commit_metadata_fails_closed(tmp_path, payload) -> None:
    head = Head(str(tmp_path))
    path = head.shallow_path()
    path.parent.mkdir(parents=True)
    path.write_text(json.dumps(payload), encoding="utf-8")

    with pytest.raises(DmlRepoError, match="Invalid shallow metadata"):
        head.get_shallow_commits()


def test_shallow_commit_metadata_rejects_non_commit_refs(tmp_path) -> None:
    with pytest.raises(ValueError, match="only exact commit refs"):
        Head(str(tmp_path)).write_shallow_commits({Ref("dag:" + "a" * 64)})
