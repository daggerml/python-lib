"""Offline migration validates graph identities and never mutates the source."""

import gzip
import json
from types import SimpleNamespace

import boto3
import pytest
from moto import mock_aws

from daggerml._core.migrate_remote import _LEGACY_REMOTE_DESCRIPTOR, _fingerprint, _inventory, main, migrate
from daggerml._core.remote import Remote
from daggerml._core.s3_cas import S3Remote
from daggerml._core.types import (
    ArgvNode,
    Commit,
    Dag,
    DmlRepoError,
    ListDatum,
    LiteralNode,
    ScalarDatum,
    Tree,
    UriDatum,
)
from tests._core.contracts.test_remote_packs_contract import database

pytestmark = pytest.mark.slow


@pytest.fixture
def legacy(tmp_path):
    with mock_aws():
        client = boto3.client("s3", region_name="us-east-1")
        client.create_bucket(Bucket="migration")
        source = S3Remote("s3://migration/old", client)
        old = object.__new__(Remote)
        old.root_uri, old.n_workers = source.s3_uri, 2
        old._store = S3Remote(source.s3_uri + "/dml", client)
        old._store._put_js(old._store._key_for("dml.json"), _LEGACY_REMOTE_DESCRIPTOR)
        db = database(tmp_path / "source-db")
        with db.tx() as txn:
            value = txn.put(ScalarDatum("done"))
            argv = txn.put(ArgvNode(value=txn.put(ListDatum([value]))))
            result = txn.put(LiteralNode(value=value))
            uri = txn.put(UriDatum("s3://migration/old/data/artifact"))
            uri_node = txn.put(LiteralNode(value=uri))
            dag = txn.put(Dag(nodes=[argv, result, uri_node], names={}, tags=[], result=result, argv=argv))
            cached = txn.put(Dag(nodes=[argv, result], names={}, tags=["cache-only"], result=result, argv=argv))
            tree = txn.put(Tree(dags={"main": dag}))
            parent = txn.put(Commit(parents=[], tree=tree, author="a", message="parent"))
            tip = txn.put(Commit(parents=[parent], tree=tree, author="a", message="tip"))
        for root in (tip, argv, cached):
            objects, _ = old._collect_local_objects(root, db)
            for key, payload in objects.items():
                old._store._put(key, payload)
        for kind in ("heads", "tags", "tombstone"):
            old._store._put_js(old._store._key_for(f"refs/{kind}/main.json"), old._build_ref_payload(tip))
        record = {
            "metadata": {"execution_id": "exec", "cache_key": "cache", "argv_ref": argv.to, "created_at": 0},
            "state": {"lifecycle": "succeeded", "result_ref": cached.to, "result_source": "runtime",
                      "spawned_execution_ids": [], "child_execution_ids": [], "cancelation": None,
                      "invalidation": None, "updated_at": 0},
            "driver": {"lock": None, "not_before": None, "adapter_state": None,
                       "cleanup": {"status": "complete", "error": None}},
        }
        for part, payload in record.items():
            source._put_js(source._key_for(f"exec/execution/exec/{part}.json"), payload)
        source._put(source._key_for("exec/cache/cache"), "exec")
        source._put(source._key_for("data/artifact"), b"\x00\xffexternal bytes\n")
        source._put(source._key_for("exec/io/exec/log"), b"log bytes\n")
        yield SimpleNamespace(source=source, client=client, old=old, tip=tip, parent=parent,
                              argv=argv, dag=dag, cached=cached, uri=uri, record=record)


def test_migration_roundtrips_project_cache_artifacts_and_source_identities(legacy, tmp_path):
    g = legacy
    before = _fingerprint(_inventory(g.source))
    stats = migrate(g.source.s3_uri, "s3://migration/new", client=g.client, workers=2)
    assert stats["graph_roots"] == 3
    assert _fingerprint(_inventory(g.source)) == before
    remote = Remote("s3://migration/new", 2, g.client)
    destination = S3Remote(remote.root_uri, g.client)
    inventory = _inventory(destination)
    assert not any(key.startswith("dml/cas/") for key in inventory)
    assert "migration-in-progress.json" not in inventory
    target = database(tmp_path / "target")
    assert remote.get_ref("branch", "main", target) == g.tip
    assert remote.get_ref("tag", "main", target) == g.tip
    assert remote.materialize_ref(g.argv, target) == g.argv
    assert remote.materialize_ref(g.cached, target) == g.cached
    with target.tx(readonly=True) as txn:
        assert txn.exists(g.parent)
        assert txn.get(g.uri).uri == "s3://migration/old/data/artifact"
    assert destination._get_bytes(destination._key_for("data/artifact")) == b"\x00\xffexternal bytes\n"
    for part, payload in g.record.items():
        assert json.loads(destination._get(destination._key_for(f"exec/execution/exec/{part}.json"))) == payload
    remote.gc()
    assert remote._store._exists(remote._pack_key(g.cached))
    assert remote._store._exists(remote._pack_key(g.argv))


def test_dry_run_does_not_write_any_objects(legacy):
    g = legacy
    before = _fingerprint(_inventory(S3Remote("s3://migration", g.client)))
    migrate(g.source.s3_uri, "s3://migration/new", client=g.client, dry_run=True)
    assert _fingerprint(_inventory(S3Remote("s3://migration", g.client))) == before


@pytest.mark.parametrize("destination", ["s3://migration/old", "s3://migration/old/child", "s3://migration"])
def test_migration_refuses_overlapping_destinations(legacy, destination):
    with pytest.raises(DmlRepoError, match="overlap"):
        migrate(legacy.source.s3_uri, destination, client=legacy.client)


def test_migration_refuses_occupied_destination(legacy):
    legacy.client.put_object(Bucket="migration", Key="new/user-file", Body=b"keep")
    with pytest.raises(DmlRepoError, match="empty"):
        migrate(legacy.source.s3_uri, "s3://migration/new", client=legacy.client)
    assert legacy.client.get_object(Bucket="migration", Key="new/user-file")["Body"].read() == b"keep"


@pytest.mark.parametrize("problem", [
    "running", "canceled", "locked", "cleanup-pending", "cleanup-failed", "partial", "stale-cache", "opaque-driver",
])
def test_unchanged_execution_records_are_copied_verbatim_without_readiness_checks(legacy, problem, tmp_path):
    g = legacy
    if problem in {"running", "canceled"}:
        g.record["state"].update(lifecycle=problem, result_ref=None, result_source=None)
        g.record["driver"].update(adapter_state={"container": "unchanged"}, cleanup=None)
        g.source._put_js(g.source._key_for("exec/execution/exec/state.json"), g.record["state"])
        g.source._put_js(g.source._key_for("exec/execution/exec/driver.json"), g.record["driver"])
    elif problem in {"locked", "cleanup-pending", "cleanup-failed"}:
        if problem == "locked":
            g.record["driver"]["lock"] = {"owner": "owner", "ttl": 300}
        elif problem == "cleanup-failed":
            g.record["driver"].update(adapter_state=None, cleanup={"status": "failed", "error": "keep this failure"})
        else:
            g.record["driver"].update(adapter_state=None, cleanup=None)
        g.source._put_js(g.source._key_for("exec/execution/exec/driver.json"), g.record["driver"])
    elif problem == "partial":
        g.source._delete(g.source._key_for("exec/execution/exec/state.json"))
    elif problem == "stale-cache":
        g.source._put(g.source._key_for("exec/cache/cache"), "missing")
    elif problem == "opaque-driver":
        g.source._put(g.source._key_for("exec/execution/exec/driver.json"), b"\x00\xffopaque driver bytes")
    g.source._put(g.source._key_for("exec/execution/exec/extra.bin"), b"\x00\xffunchanged auxiliary bytes")
    before = _fingerprint(_inventory(g.source))
    unchanged = {key: g.source._get_bytes(g.source._key_for(key)) for key in before
                 if key != "dml/dml.json" and not key.startswith("dml/cas/")}
    migrate(g.source.s3_uri, "s3://migration/new", client=g.client)
    assert _fingerprint(_inventory(g.source)) == before
    destination = S3Remote("s3://migration/new", g.client)
    for key, original in unchanged.items():
        assert destination._get_bytes(destination._key_for(key)) == original
    remote = Remote("s3://migration/new", 2, g.client)
    target = database(tmp_path / "target")
    assert remote.get_ref("branch", "main", target) == g.tip
    assert remote.materialize_ref(g.argv, target) == g.argv
    if problem not in {"running", "canceled", "partial"}:
        assert remote.materialize_ref(g.cached, target) == g.cached


def test_corrupt_graph_cannot_publish_destination(legacy):
    g = legacy
    g.old._store._put(g.old._cas_key(g.argv.id()), g.old._dump_cas_object(ArgvNode(value=g.uri)))
    before = _fingerprint(_inventory(g.source))
    with pytest.raises((ValueError, DmlRepoError)):
        migrate(g.source.s3_uri, "s3://migration/new", client=g.client)
    assert _fingerprint(_inventory(g.source)) == before
    assert not S3Remote("s3://migration/new", g.client)._exists("new/dml/dml.json")


def test_source_mutation_leaves_unpublished_staging_destination(legacy, monkeypatch):
    g = legacy
    original = g.client.copy

    def mutate(*args, **kwargs):
        original(*args, **kwargs)
        g.source._put(g.source._key_for("data/changed"), b"mutation")

    monkeypatch.setattr(g.client, "copy", mutate)
    with pytest.raises(DmlRepoError, match="Source changed"):
        migrate(g.source.s3_uri, "s3://migration/new", client=g.client)
    with pytest.raises(DmlRepoError, match="not empty"):
        Remote("s3://migration/new", 2, g.client)


def test_verification_reads_standalone_argv_pack_even_if_also_in_a_dag(legacy, monkeypatch):
    original = Remote._put_immutable

    def corrupt_argv(remote, key, value):
        original(remote, key, value)
        if "/packs/argv/" in key:
            remote._store._put(key, b"corrupt gzip")

    monkeypatch.setattr(Remote, "_put_immutable", corrupt_argv)
    before = _fingerprint(_inventory(legacy.source))
    with pytest.raises(gzip.BadGzipFile):
        migrate(legacy.source.s3_uri, "s3://migration/new", client=legacy.client)
    assert _fingerprint(_inventory(legacy.source)) == before
    assert not S3Remote("s3://migration/new", legacy.client)._exists("new/dml/dml.json")


def test_cli_requires_explicit_quiescence_confirmation(monkeypatch):
    monkeypatch.setattr("sys.argv", ["migrate_remote", "--source", "s3://bucket/old",
                                     "--destination", "s3://bucket/new"])
    with pytest.raises(SystemExit) as error:
        main()
    assert error.value.code == 2
