import json

import boto3
import pytest
from moto import mock_aws

from daggerml._core.remote import Remote
from daggerml._core.types import ArgvNode, Commit, Dag, DmlDB, ListDatum, LiteralNode, ScalarDatum, Tree


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
            tree = txn.put(Tree(dags={"main": dag_ref}))
            commit_ref = txn.put(Commit(parents=[], tree=tree, author="alice", message="snapshot"))

        cache_key = remote.put_cache(dag_ref, "exec-1", source_db)
        loaded_dag = remote.get_cache(cache_key, target_db)
        remote.put_ref(commit_ref, "acme", "demo", "branch", "main", source_db)
        loaded_commit = remote.get_ref("acme", "demo", "branch", "main", target_db)

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
            tree = txn.put(Tree(dags={"main": dag_ref}))
            commit_ref = txn.put(Commit(parents=[], tree=tree, author="alice", message="snapshot"))

        cache_key = remote.put_cache(dag_ref, "exec-1", source_db)
        remote.put_active(cache_key, "exec-1", argv_node, source_db)
        remote.put_transport("exec-1", dag_ref, source_db)
        remote.put_ref(commit_ref, "acme", "demo", "branch", "main", source_db)

        cache_payload = remote._read_ref(remote._cache_key(cache_key))
        active_payload = remote._read_ref(remote._active_key(cache_key))
        transport_payload = remote._read_ref(remote._transport_key("exec-1"))
        project_payload = remote._read_ref(remote._ref_key("acme", "demo", "branch", "main"))
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
