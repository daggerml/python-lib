import json

import boto3
import pytest
from moto import mock_aws

from daggerml._core.remote import Remote
from daggerml._core.types import ArgvNode, Commit, Dag, DmlDB, DmlRepoError, ListDatum, LiteralNode, ScalarDatum, Tree


def make_db(path):
    path.mkdir()
    db = DmlDB(str(path), 1024 * 1024, 1024 * 1024 * 64)
    with db.tx(create_if_missing=True):
        pass
    return db


def test_cache_roundtrip(tmp_path):
    with mock_aws():
        client = boto3.client("s3", region_name="us-east-1")
        client.create_bucket(Bucket="bucket")
        remote = Remote("s3://bucket/root", n_workers=2, client=client)
        source_db = make_db(tmp_path / "source-db")
        target_db = make_db(tmp_path / "target-db")

        with source_db.tx() as txn:
            argv_value = txn.put(ListDatum([]))
            argv_node = txn.put(ArgvNode(value=argv_value))
            value = txn.put(ScalarDatum("done"))
            result = txn.put(LiteralNode(value=value))
            dag_ref = txn.put(Dag(nodes=[argv_node, result], names={"result": result}, result=result, argv=argv_node))
            tree = txn.put(Tree(dags={"main": dag_ref}, tags={}))
            commit_ref = txn.put(Commit(parents=[], tree=tree, author="alice", message="snapshot"))

        cache_key = remote.put_cache(dag_ref, "exec-1", source_db)
        loaded_dag = remote.get_cache(cache_key, target_db)
        remote.put_ref(commit_ref, "branch", "main", source_db)
        loaded_commit = remote.get_ref("branch", "main", target_db)

        assert cache_key == argv_value.id()
        assert loaded_dag == dag_ref
        assert loaded_commit == commit_ref

        with target_db.tx() as txn:
            dag = txn.get(loaded_dag)
            node = txn.get(dag.result)
            datum = txn.get(node.value)
            commit = txn.get(loaded_commit)

        assert datum.data == "done"
        assert commit.tree == tree


def test_remote_descriptor_initializes_empty_root_and_rejects_undescribed_state():
    with mock_aws():
        client = boto3.client("s3", region_name="us-east-1")
        client.create_bucket(Bucket="bucket")

        remote = Remote("s3://bucket/empty", n_workers=2, client=client)
        assert json.loads(remote._store._get(remote._store._key_for("dml.json"))) == {
            "cas_prefix": "cas/sha256",
            "hash": "sha256",
            "io_prefix": "io",
            "layout": "one-project-cas+refs",
            "refs_prefix": "refs",
            "schema": 1,
        }

        client.put_object(Bucket="bucket", Key="nonempty/dml/refs/heads/main.json", Body=b"{}")
        with pytest.raises(DmlRepoError, match="not empty"):
            Remote("s3://bucket/nonempty", n_workers=2, client=client)


def test_manifest_fetch_precedes_replayable_local_write(tmp_path, monkeypatch):
    with mock_aws():
        client = boto3.client("s3", region_name="us-east-1")
        client.create_bucket(Bucket="bucket")
        remote = Remote("s3://bucket/root", n_workers=2, client=client)
        source_db = make_db(tmp_path / "source-db")
        target_db = make_db(tmp_path / "target-db")
        with source_db.tx() as txn:
            argv_value = txn.put(ListDatum([]))
            argv = txn.put(ArgvNode(value=argv_value))
            value = txn.put(ScalarDatum("done"))
            result = txn.put(LiteralNode(value=value))
            dag_ref = txn.put(Dag(nodes=[argv, result], names={"result": result}, result=result, argv=argv))
        cache_key = remote.put_cache(dag_ref, "exec-1", source_db)

        in_local_write = False
        original_get = remote._store._get
        original_write = target_db.write_with_growth

        def guarded_get(*args, **kwargs):
            assert not in_local_write
            return original_get(*args, **kwargs)

        def write_with_tracking(fn):
            nonlocal in_local_write
            in_local_write = True
            try:
                return original_write(fn)
            finally:
                in_local_write = False

        monkeypatch.setattr(remote._store, "_get", guarded_get)
        monkeypatch.setattr(target_db, "write_with_growth", write_with_tracking)

        assert remote.get_cache(cache_key, target_db) == dag_ref


def test_remote_materialization_skips_cas_fetch_for_local_root(tmp_path, monkeypatch):
    with mock_aws():
        client = boto3.client("s3", region_name="us-east-1")
        client.create_bucket(Bucket="bucket")
        remote = Remote("s3://bucket/root", n_workers=2, client=client)
        target_db = make_db(tmp_path / "target-db")
        with target_db.tx() as txn:
            root_ref = txn.put(ScalarDatum("local"))

        def fail_unexpected_cas_fetch(*args, **kwargs):
            raise AssertionError("local root should not fetch remote CAS objects")

        monkeypatch.setattr(remote._store, "_get", fail_unexpected_cas_fetch)

        assert remote.materialize_manifest(
            {"ref": {"to": root_ref.to}, "created": 0, "metadata": {}}, target_db
        ) == root_ref


@pytest.mark.slow
def test_remote_materialization_grows_map_for_large_dag_payload(tmp_path):
    with mock_aws():
        client = boto3.client("s3", region_name="us-east-1")
        client.create_bucket(Bucket="bucket")
        remote = Remote("s3://bucket/root", n_workers=2, client=client)
        source_db = make_db(tmp_path / "source-db")
        target_db = make_db(tmp_path / "target-db")

        def write_large_dag(txn):
            argv_value = txn.put(ListDatum([]))
            argv = txn.put(ArgvNode(value=argv_value))
            nodes = [argv]
            for i in range(12):
                value = txn.put(ScalarDatum(f"{i:05d}" + "x" * (900 * 1024 - 5)))
                nodes.append(txn.put(LiteralNode(value=value)))
            return txn.put(Dag(nodes=nodes, names={}, result=nodes[-1], argv=argv))

        dag_ref = source_db.write_with_growth(write_large_dag)
        cache_key = remote.put_cache(dag_ref, "exec-large", source_db)

        assert remote.get_cache(cache_key, target_db) == dag_ref


def test_remote_ref_payloads_use_typed_roots(tmp_path):
    with mock_aws():
        client = boto3.client("s3", region_name="us-east-1")
        client.create_bucket(Bucket="bucket")
        remote = Remote("s3://bucket/root", n_workers=2, client=client)
        source_db = make_db(tmp_path / "source-db")

        with source_db.tx() as txn:
            argv_value = txn.put(ListDatum([]))
            argv_node = txn.put(ArgvNode(value=argv_value))
            value = txn.put(ScalarDatum("done"))
            result = txn.put(LiteralNode(value=value))
            dag_ref = txn.put(Dag(nodes=[argv_node, result], names={"result": result}, result=result, argv=argv_node))
            tree = txn.put(Tree(dags={"main": dag_ref}, tags={}))
            commit_ref = txn.put(Commit(parents=[], tree=tree, author="alice", message="snapshot"))

        cache_key = remote.put_cache(dag_ref, "exec-1", source_db)
        remote.put_active(cache_key, "exec-1", argv_node, source_db)
        remote.put_transport("exec-1", dag_ref, source_db)
        remote.put_ref(commit_ref, "branch", "main", source_db)

        cache_payload = remote._read_ref(remote._cache_key(cache_key))
        active_payload = remote._read_ref(remote._active_key(cache_key))
        transport_payload = remote._read_ref(remote._transport_key("exec-1"))
        project_payload = remote._read_ref(remote._ref_key("branch", "main"))
        cache_raw = remote.get_cache(cache_key, raw=True)
        active_raw = remote.get_active(cache_key, raw=True)

        assert set(cache_payload) == {"ref", "created", "metadata"}
        assert cache_payload["ref"] == {"to": dag_ref.to}
        assert cache_payload["metadata"] == {"execution_id": "exec-1"}
        assert cache_raw["meta"]["execution_id"] == "exec-1"

        assert set(active_payload) == {"ref", "created", "metadata"}
        assert active_payload["ref"] == {"to": argv_node.to}
        assert active_payload["metadata"] == {"execution_id": "exec-1"}
        assert active_raw["meta"]["execution_id"] == "exec-1"

        assert set(transport_payload) == {"ref", "created", "metadata"}
        assert transport_payload["ref"] == {"to": dag_ref.to}
        assert transport_payload["metadata"] == {"ts": transport_payload["metadata"]["ts"]}

        assert set(project_payload) == {"ref", "created", "metadata"}
        assert project_payload["ref"] == {"to": commit_ref.to}
        assert project_payload["metadata"] == {}


def test_remote_cas_payloads_use_tagged_json(tmp_path):
    with mock_aws():
        client = boto3.client("s3", region_name="us-east-1")
        client.create_bucket(Bucket="bucket")
        remote = Remote("s3://bucket/root", n_workers=2, client=client)
        source_db = make_db(tmp_path / "source-db")

        with source_db.tx() as txn:
            argv_value = txn.put(ListDatum([]))
            argv_node = txn.put(ArgvNode(value=argv_value))
            value = txn.put(ScalarDatum("done"))
            result = txn.put(LiteralNode(value=value))
            dag_ref = txn.put(Dag(nodes=[argv_node, result], names={"result": result}, result=result, argv=argv_node))

        remote.put_cache(dag_ref, "exec-1", source_db)

        dag_payload = json.loads(remote._store._get(remote._cas_key(dag_ref.id())))
        scalar_payload = json.loads(remote._store._get(remote._cas_key(value.id())))

        assert dag_payload == [
            "dict",
            {
                "argv": ["ref", argv_node.to],
                "error": ["scalar", None],
                "names": ["dict", {"result": ["ref", result.to]}],
                "nodes": ["list", [["ref", argv_node.to], ["ref", result.to]]],
                "result": ["ref", result.to],
            },
        ]
        assert scalar_payload == ["dict", {"data": ["scalar", "done"]}]


def test_remote_pull_rejects_cas_identity_mismatch(tmp_path):
    with mock_aws():
        client = boto3.client("s3", region_name="us-east-1")
        client.create_bucket(Bucket="bucket")
        remote = Remote("s3://bucket/root", n_workers=2, client=client)
        source_db = make_db(tmp_path / "source-db")
        target_db = make_db(tmp_path / "target-db")

        with source_db.tx() as txn:
            argv_value = txn.put(ListDatum([]))
            argv_node = txn.put(ArgvNode(value=argv_value))
            value = txn.put(ScalarDatum("done"))
            result = txn.put(LiteralNode(value=value))
            dag_ref = txn.put(Dag(nodes=[argv_node, result], names={"result": result}, result=result, argv=argv_node))

        cache_key = remote.put_cache(dag_ref, "exec-1", source_db)
        remote._store._put(remote._cas_key(value.id()), remote._dump_cas_object(ScalarDatum("different")))

        with pytest.raises(ValueError, match="Remote CAS object identity mismatch"):
            remote.get_cache(cache_key, target_db)


def test_deleting_ref_moves_original_payload_to_tombstone(tmp_path):
    with mock_aws():
        client = boto3.client("s3", region_name="us-east-1")
        client.create_bucket(Bucket="bucket")
        remote = Remote("s3://bucket/root", n_workers=2, client=client)
        source_db = make_db(tmp_path / "source-db")

        with source_db.tx() as txn:
            argv_value = txn.put(ListDatum([]))
            argv_node = txn.put(ArgvNode(value=argv_value))

        remote.put_active("cache-1", "exec-1", argv_node, source_db)
        active_path = remote._active_key("cache-1")
        original = json.dumps(remote._read_ref(active_path), sort_keys=True)

        assert remote.delete_active("cache-1") is True

        tombstones = list(remote._store._iter(remote._store._key_for("refs/tombstone/")))
        assert len(tombstones) == 1
        assert json.dumps(json.loads(remote._store._get(tombstones[0])), sort_keys=True) == original


def test_non_forced_branch_push_ref_rejects_create_and_update_races(tmp_path, monkeypatch):
    with mock_aws():
        client = boto3.client("s3", region_name="us-east-1")
        client.create_bucket(Bucket="bucket")
        remote = Remote("s3://bucket/root", n_workers=2, client=client)
        db = make_db(tmp_path / "db")
        with db.tx() as txn:
            tree = txn.put(Tree(dags={}, tags={}))
            base = txn.put(Commit(tree=tree, parents=[], author="alice", message="base"))
            candidate = txn.put(Commit(tree=tree, parents=[base], author="alice", message="candidate"))
            racer = txn.put(Commit(tree=tree, parents=[base], author="bob", message="racer"))

        ref_path = remote._ref_key("branch", "main")
        original_put_cas = remote._put_cas

        def race_create(ref, path, ref_db, **kwargs):
            if path == ref_path and kwargs.get("exists_ok") is False:
                original_put_cas(racer, ref_path, ref_db)
            return original_put_cas(ref, path, ref_db, **kwargs)

        monkeypatch.setattr(remote, "_put_cas", race_create)
        with pytest.raises(DmlRepoError, match="updated concurrently"):
            remote.put_ref(candidate, "branch", "main", db)
        assert remote.get_ref("branch", "main", db) == racer

        monkeypatch.setattr(remote, "_put_cas", original_put_cas)
        remote.put_ref(base, "branch", "other", db)
        update_path = remote._ref_key("branch", "other")

        def race_update(ref, path, ref_db, **kwargs):
            if isinstance(path, str):
                return original_put_cas(ref, path, ref_db, **kwargs)
            original_put_cas(racer, update_path, ref_db)
            return original_put_cas(ref, path, ref_db, **kwargs)

        monkeypatch.setattr(remote, "_put_cas", race_update)
        with pytest.raises(DmlRepoError, match="updated concurrently"):
            remote.put_ref(candidate, "branch", "other", db)
        assert remote.get_ref("branch", "other", db) == racer
