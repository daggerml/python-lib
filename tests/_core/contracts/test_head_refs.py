from __future__ import annotations

from tempfile import TemporaryDirectory

import pytest
from hypothesis import given, settings

from daggerml._core.db import Ref
from daggerml._core.head import Head, _validate_ref_name, _validate_segment
from daggerml._core.types import DmlRepoError
from daggerml._core.uri import ProjectUri
from tests._core.strategies import nested_ref_names, project_segments, project_uris


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


@given(project_segments, project_segments, nested_ref_names)
@settings(max_examples=35, deadline=None)
def test_remote_refs_validate_and_round_trip(owner: str, project: str, name: str) -> None:
    with TemporaryDirectory() as root:
        head = Head(root)
        commit = Ref("commit:" + "b" * 64)

        head.create_remote_ref(owner, project, name, commit, kind="tag")

        assert head.get_remote_ref(owner, project, name, kind="tag") == commit
        assert head.list_remote_refs(owner, project, kind="tag") == [name]


@given(project_uris)
@settings(max_examples=35, deadline=None)
def test_accepted_project_uris_parse_and_stringify(uri: str) -> None:
    parsed = ProjectUri.from_uri(uri)

    assert str(parsed) == uri
    assert parsed.ensure_project().owner == parsed.owner


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


def test_remote_branch_refs_round_trip_update_and_duplicate_rejection(tmp_path) -> None:
    head = Head(str(tmp_path))
    first = Ref("commit:" + "5" * 64)
    second = Ref("commit:" + "6" * 64)

    head.create_remote_ref("acme", "demo", "main", first)
    assert head.get_remote_ref("acme", "demo", "main") == first
    assert head.list_remote_projects() == [("acme", "demo")]
    assert head.list_remote_refs("acme", "demo") == ["main"]

    with pytest.raises(DmlRepoError, match="Branch already exists"):
        head.create_remote_ref("acme", "demo", "main", second)

    head.update_remote_ref("acme", "demo", "main", second)
    assert head.get_remote_ref("acme", "demo", "main") == second

    head.delete_remote_ref("acme", "demo", "main")
    assert head.list_remote_refs("acme", "demo") == []


def test_branch_upstream_lifecycle_follows_branch_rename_and_delete(tmp_path) -> None:
    head = Head(str(tmp_path))
    commit = Ref("commit:" + "7" * 64)
    head.create_local_ref("feature", commit)

    assert head.get_upstream("feature") is None
    assert head.set_upstream("feature", "origin", "main") == {"remote": "origin", "merge": "main"}
    head.rename_local_ref("feature", "review")
    assert head.get_upstream("feature") is None
    assert head.get_upstream("review") == {"remote": "origin", "merge": "main"}

    head.delete_local_ref("review")
    assert head.get_upstream("review") is None


def test_named_remote_tracking_refs_migrate_legacy_origin_and_enumerate_gc_roots(tmp_path) -> None:
    head = Head(str(tmp_path))
    branch = Ref("commit:" + "8" * 64)
    tag = Ref("commit:" + "9" * 64)
    head.create_remote_ref("acme", "demo", "main", branch)
    head.create_remote_ref("acme", "demo", "v1", tag, kind="tag")

    head.migrate_legacy_remote_refs("origin", "acme", "demo")

    assert head.get_remote_tracking_ref("origin", "main") == branch
    assert head.list_remote_tracking_refs("origin", kind="tag") == ["v1"]
    assert set(head.iter_all_remote_tracking_refs()) == {branch, tag}
