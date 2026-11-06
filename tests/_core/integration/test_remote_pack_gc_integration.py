"""GC traces manifests, retains live/young objects, and batch-deletes garbage."""

import gzip
import json

import boto3
import pytest
from moto import mock_aws

from daggerml._core.remote import Remote
from daggerml._core.types import Commit, Dag, DmlRepoError, LiteralNode, ScalarDatum, Tree
from tests._core.contracts.test_remote_packs_contract import database

pytestmark = pytest.mark.slow


@pytest.fixture
def storage(tmp_path):
    with mock_aws():
        client = boto3.client("s3", region_name="us-east-1")
        client.create_bucket(Bucket="gc-check")
        remote = Remote("s3://gc-check/root", 2, client)
        db = database(tmp_path / "db")
        with db.tx() as txn:
            value = txn.put(ScalarDatum("value"))
            node = txn.put(LiteralNode(value=value))
            dag = txn.put(Dag(nodes=[node], names={}, tags=[], result=node))
            orphan = txn.put(Dag(nodes=[node], names={}, tags=["orphan"], result=node))
            tree = txn.put(Tree(dags={"main": dag}))
            commit = txn.put(Commit(parents=[], tree=tree, author="a", message="snapshot"))
        remote.put_ref(commit, "branch", "main", db)
        remote.upload_object_graph(orphan, db)
        yield remote, dag, orphan, commit, value


def test_gc_retains_young_unpublished_packs_then_deletes_only_orphans(storage):
    remote, dag, orphan, commit, _ = storage
    stats = remote.gc()
    assert stats["cas-retained"] == 5
    assert remote._store._exists(remote._pack_key(orphan))
    remote.prune_age_seconds = 0
    stats = remote.gc()
    assert stats["cas-deleted"] == 2
    assert not remote._store._exists(remote._pack_key(orphan))
    assert remote._store._exists(remote._pack_key(dag))
    assert remote._store._exists(remote._commit_key(commit))


@pytest.mark.parametrize("damage", ["missing", "identity", "closure", "gzip"])
def test_gc_does_not_inspect_live_pack_contents_or_check_their_existence(storage, damage, monkeypatch):
    remote, dag, orphan, _, value = storage
    remote.prune_age_seconds = 0
    if damage == "missing":
        remote._store._delete(remote._pack_key(dag))
    elif damage == "gzip":
        remote._store._put(remote._pack_key(dag), b"not gzip")
    else:
        pack = json.loads(gzip.decompress(remote._store._get_bytes(remote._pack_key(dag))))
        if damage == "identity":
            pack["objects"][value.to] = remote._dump_cas_object(ScalarDatum("changed"))
        else:
            del pack["objects"][value.to]
        remote._store._put(remote._pack_key(dag), gzip.compress(json.dumps(pack).encode()))
    def unexpected(*args, **kwargs):
        pytest.fail("GC must not read packs, check object existence, or open a database")

    with monkeypatch.context() as patch:
        patch.setattr(remote._store, "_get_bytes", unexpected)
        patch.setattr(remote._store.client, "head_object", unexpected)
        patch.setattr("daggerml._core.remote.DmlDB", unexpected)
        remote.gc()
    assert not remote._store._exists(remote._pack_key(orphan))


def test_recent_tombstones_keep_graphs_until_grace_period_expires(storage):
    remote, dag, orphan, commit, _ = storage
    remote.delete_ref("branch", "main")
    remote.gc()
    assert remote._store._exists(remote._pack_key(dag))
    remote.prune_age_seconds = 0
    stats = remote.gc()
    assert stats["tombstones-deleted"] == 1
    assert not remote._store._exists(remote._pack_key(dag))
    assert not remote._store._exists(remote._pack_key(orphan))
    assert not remote._store._exists(remote._commit_key(commit))


@pytest.mark.parametrize("stray", ["packs/", "packs/dag/notes.txt", "manifests/dag/not-a-hash.json"])
def test_unrecognized_objects_raise_with_uri_and_delete_nothing_by_default(storage, stray):
    remote, dag, orphan, _, _ = storage
    remote.prune_age_seconds = 0
    key = remote._store._key_for(stray)
    remote._store.client.put_object(Bucket=remote._store.bucket, Key=key, Body=b"")
    with pytest.raises(DmlRepoError, match=f"s3://{remote._store.bucket}/{key}"):
        remote.gc()
    assert remote._store._exists(remote._pack_key(orphan))
    assert remote._store._exists(key)


def test_ignored_unrecognized_objects_are_logged_kept_and_do_not_block_gc(storage, caplog):
    remote, dag, orphan, commit, _ = storage
    remote.prune_age_seconds = 0
    key = remote._store._key_for("packs/dag/notes.txt")
    remote._store.client.put_object(Bucket=remote._store.bucket, Key=key, Body=b"keep me")
    with caplog.at_level("WARNING", logger="daggerml._core.remote"):
        remote.gc(unrecognized="ignore")
    assert f"s3://{remote._store.bucket}/{key}" in caplog.text
    assert remote._store._exists(key)
    assert not remote._store._exists(remote._pack_key(orphan))
    assert remote._store._exists(remote._pack_key(dag))
    assert remote._store._exists(remote._commit_key(commit))


def test_gc_rejects_unknown_unrecognized_policy(storage):
    with pytest.raises(ValueError, match="unrecognized"):
        storage[0].gc(unrecognized="delete")  # type: ignore[arg-type]


@pytest.mark.parametrize("damage", ["missing", "unreadable"])
def test_unreadable_dependency_metadata_prevents_sweeping(storage, damage):
    remote, dag, orphan, _, _ = storage
    remote.prune_age_seconds = 0
    if damage == "missing":
        remote._store._delete(remote._manifest_key(dag))
        error = remote._store.client.exceptions.NoSuchKey
    else:
        remote._store._put(remote._manifest_key(dag), "not JSON")
        error = json.JSONDecodeError
    with pytest.raises(error):
        remote.gc()
    assert remote._store._exists(remote._pack_key(orphan))


def test_gc_batches_graph_and_expired_tombstone_deletions(storage, monkeypatch):
    remote, dag, orphan, commit, _ = storage
    remote.prune_age_seconds = 0
    remote.delete_ref("branch", "main")
    tombstones = list(remote._store._iter(remote._store._key_for("refs/tombstone/")))
    deleted = []
    original = remote._store.client.delete_objects

    def delete_objects(**kwargs):
        deleted.extend(obj["Key"] for obj in kwargs["Delete"]["Objects"])
        return original(**kwargs)

    def unexpected(*args, **kwargs):
        pytest.fail("Graph garbage and expired tombstones must use batch deletion")

    monkeypatch.setattr(remote._store.client, "delete_objects", delete_objects)
    monkeypatch.setattr(remote._store.client, "delete_object", unexpected)
    stats = remote.gc()
    expected = {remote._pack_key(ref) for ref in (dag, orphan)}
    expected.update(remote._manifest_key(ref) for ref in (dag, orphan))
    expected.add(remote._commit_key(commit))
    assert set(deleted) == expected | set(tombstones)
    assert len(deleted) == len(set(deleted))
    assert stats["cas-deleted"] == len(expected)
    assert stats["tombstones-deleted"] == len(tombstones)
