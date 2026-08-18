import json

import boto3
import pytest
from moto import mock_aws

from daggerml._core.db import Ref
from daggerml._core.exec_state import ExecutionState
from daggerml._core.head import Head
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

        remote.upload_object_graph(dag_ref, source_db)
        loaded_dag = remote.materialize_ref(dag_ref, target_db)
        remote.put_ref(commit_ref, "branch", "main", source_db)
        loaded_commit = remote.get_ref("branch", "main", target_db)

        assert loaded_dag == dag_ref
        assert loaded_commit == commit_ref

        with target_db.tx() as txn:
            dag = txn.get(loaded_dag)
            node = txn.get(dag.result)
            datum = txn.get(node.value)
            commit = txn.get(loaded_commit)

        assert datum.data == "done"
        assert commit.tree == tree


def test_gc_preserves_locked_reservation_and_rereads_cache_pointer_before_deletion(monkeypatch):
    with mock_aws():
        client = boto3.client("s3", region_name="us-east-1")
        client.create_bucket(Bucket="bucket")
        remote = Remote("s3://bucket/root", n_workers=1, client=client)
        exec_store = remote._store.__class__("s3://bucket/root/exec", client)
        locked = {"execution_id": "locked", "cache_key": "locked-key", "lock": {"owner": "owner", "ttl": 300}}
        unlocked = {"execution_id": "unlocked", "cache_key": "unlocked-key", "lock": None}
        exec_store._put_js(exec_store._key_for("execution/locked.json"), locked)
        exec_store._put_js(exec_store._key_for("execution/unlocked.json"), unlocked)
        original_get = exec_store.__class__._get

        def publish_pointer_after_snapshot(store, key, *, cas=False):
            item = original_get(store, key, cas=cas)
            if key.endswith("execution/unlocked.json") and cas:
                store._put(store._key_for("cache/unlocked-key"), "unlocked", overwrite=False)
            return item

        monkeypatch.setattr(exec_store.__class__, "_get", publish_pointer_after_snapshot)

        remote.gc()

        assert json.loads(exec_store._get(exec_store._key_for("execution/locked.json")))["execution_id"] == "locked"
        assert json.loads(exec_store._get(exec_store._key_for("execution/unlocked.json")))["execution_id"] == "unlocked"


def test_gc_retains_refs_when_conditional_execution_deletion_conflicts(monkeypatch):
    with mock_aws():
        client = boto3.client("s3", region_name="us-east-1")
        client.create_bucket(Bucket="bucket")
        remote = Remote("s3://bucket/root", n_workers=1, client=client)
        exec_store = remote._store.__class__("s3://bucket/root/exec", client)
        execution_key = exec_store._key_for("execution/unlocked.json")
        result = Ref("dag:surviving-result")
        exec_store._put_js(execution_key, {"execution_id": "unlocked", "cache_key": "unlocked-key"})
        remote._store._put(remote._store._key_for(f"cas/sha256/{result.id()}"), "result")
        original_delete = exec_store.__class__._delete
        mutated = False

        def mutate_before_conditional_delete(store, key, **kwargs):
            nonlocal mutated
            if not mutated and getattr(key, "key", key) == execution_key:
                mutated = True
                store._put_js(
                    execution_key,
                    {"execution_id": "unlocked", "cache_key": "unlocked-key", "result_ref": result.to},
                )
            return original_delete(store, key, **kwargs)

        monkeypatch.setattr(exec_store.__class__, "_delete", mutate_before_conditional_delete)
        monkeypatch.setattr(remote, "_get_live_oids", lambda ref: {ref.id()})

        remote.gc()

        assert remote._store._exists(remote._store._key_for(f"cas/sha256/{result.id()}"))


def test_remote_descriptor_initializes_empty_root_and_rejects_undescribed_state():
    with mock_aws():
        client = boto3.client("s3", region_name="us-east-1")
        client.create_bucket(Bucket="bucket")

        remote = Remote("s3://bucket/empty", n_workers=2, client=client)
        assert json.loads(remote._store._get(remote._store._key_for("dml.json"))) == {
            "cas_prefix": "cas/sha256",
            "hash": "sha256",
            "io_prefix": "io",
            "layout": "one-project-cas+refs+unified-execution",
            "refs_prefix": "refs",
            "schema": 2,
            "execution_prefix": "../exec",
        }

        client.put_object(Bucket="bucket", Key="nonempty/dml/refs/heads/main.json", Body=b"{}")
        with pytest.raises(DmlRepoError, match="not empty"):
            Remote("s3://bucket/nonempty", n_workers=2, client=client)


def test_remote_inspection_is_bounded_non_mutating_and_lists_unmaterialized_tips(tmp_path, monkeypatch):
    with mock_aws():
        client = boto3.client("s3", region_name="us-east-1")
        client.create_bucket(Bucket="bucket")
        calls = []
        original_list = client.list_objects_v2

        def list_objects_v2(**kwargs):
            calls.append(kwargs)
            return original_list(**kwargs)

        monkeypatch.setattr(client, "list_objects_v2", list_objects_v2)
        empty = Remote("s3://bucket/empty", n_workers=2, client=client, initialize=False)
        assert calls == [{"Bucket": "bucket", "Prefix": "empty/", "MaxKeys": 1}]
        calls.clear()
        assert empty.list_ref_tips() == []
        assert calls == [{"Bucket": "bucket", "Prefix": "empty/dml/refs/heads/"}]
        with pytest.raises(client.exceptions.NoSuchKey):
            empty._store._get(empty._store._key_for("dml.json"))

        client.put_object(Bucket="bucket", Key="nonempty/legacy", Body=b"legacy")
        calls.clear()
        with pytest.raises(DmlRepoError, match="not empty"):
            Remote("s3://bucket/nonempty", n_workers=2, client=client, initialize=False)
        assert calls == [{"Bucket": "bucket", "Prefix": "nonempty/", "MaxKeys": 1}]

        writer = Remote("s3://bucket/root", n_workers=2, client=client)
        tip = Ref("commit:" + "a" * 64)
        client.put_object(
            Bucket="bucket",
            Key=writer._store._key_for("refs/heads/main.json"),
            Body=json.dumps({"ref": {"to": tip.to}, "created": 0, "metadata": {}}).encode(),
        )
        head = Head(str(tmp_path))
        tracked_tip = Ref("commit:" + "b" * 64)
        head.update_remote_tracking_ref("main", tracked_tip)

        remote = Remote("s3://bucket/root", n_workers=2, client=client, initialize=False)
        original_get = remote._store._get

        def no_cas_reads(key, **kwargs):
            assert "/cas/sha256/" not in key
            return original_get(key, **kwargs)

        monkeypatch.setattr(remote._store, "_get", no_cas_reads)
        assert remote.list_ref_tips() == [("main", tip)]
        assert head.get_remote_tracking_ref("main") == tracked_tip
        assert list(remote._store._iter(remote._store._key_for("cas/sha256/"))) == []


@pytest.mark.parametrize(
    "payload",
    [
        {"ref": {"to": "commit:" + "a" * 64}},
        {"ref": {"to": "dag:" + "a" * 64}, "created": 0, "metadata": {}},
        {"ref": {"to": "commit:not-a-hash"}, "created": 0, "metadata": {}},
    ],
)
def test_remote_inspection_fails_closed_for_invalid_commit_refs(payload):
    with mock_aws():
        client = boto3.client("s3", region_name="us-east-1")
        client.create_bucket(Bucket="bucket")
        writer = Remote("s3://bucket/root", n_workers=2, client=client)
        client.put_object(
            Bucket="bucket",
            Key=writer._store._key_for("refs/heads/main.json"),
            Body=json.dumps(payload).encode(),
        )

        remote = Remote("s3://bucket/root", n_workers=2, client=client, initialize=False)
        with pytest.raises(DmlRepoError, match="Invalid remote branch ref"):
            remote.list_ref_tips()


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
        remote.upload_object_graph(dag_ref, source_db)

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

        assert remote.materialize_ref(dag_ref, target_db) == dag_ref


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
        remote.upload_object_graph(dag_ref, source_db)

        assert remote.materialize_ref(dag_ref, target_db) == dag_ref


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

        remote.upload_object_graph(argv_node, source_db)
        remote.upload_object_graph(dag_ref, source_db)
        state = ExecutionState("s3://bucket/root", 2, cache_key=argv_value.id(), client=client)
        execution_id, owner, _ = state.reserve_execution(argv_node, execution_id="exec-1")
        assert state._create_cache(argv_value.id(), execution_id)
        state._mutate(
            execution_id,
            owner,
            lambda record: record.update(lifecycle="succeeded", result_ref=dag_ref.to),
        )
        state.unlock(execution_id, owner)
        remote.put_ref(commit_ref, "branch", "main", source_db)

        project_payload = remote._read_ref(remote._ref_key("branch", "main"))
        assert state._read_cache(argv_value.id())[0] == "exec-1"
        record = state.read_execution_record("exec-1")
        assert record["argv_ref"] == argv_node.to
        assert record["result_ref"] == dag_ref.to

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

        remote.upload_object_graph(dag_ref, source_db)

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


def test_remote_gc_traces_unified_execution_refs_and_collects_losing_attempt(tmp_path):
    with mock_aws():
        client = boto3.client("s3", region_name="us-east-1")
        client.create_bucket(Bucket="bucket")
        remote = Remote("s3://bucket/root", n_workers=2, client=client)
        source_db = make_db(tmp_path / "source-db")
        with source_db.tx() as txn:
            argv = txn.put(ArgvNode(value=txn.put(ListDatum([]))))
            result = txn.put(LiteralNode(value=txn.put(ScalarDatum("done"))))
            dag_ref = txn.put(Dag(nodes=[argv, result], names={}, result=result, argv=argv))
        remote.upload_object_graph(dag_ref, source_db)

        state = ExecutionState("s3://bucket/root", 2, cache_key="current", client=client)
        execution_id, owner, _ = state.reserve_execution(argv, execution_id="current-exec")
        assert state._create_cache("current", execution_id)
        state._mutate(
            execution_id,
            owner,
            lambda record: record.update(lifecycle="succeeded", result_ref=dag_ref.to),
        )
        state.unlock(execution_id, owner)
        losing = ExecutionState("s3://bucket/root", 2, cache_key="lost", client=client)
        _, losing_owner, _ = losing.reserve_execution(argv, execution_id="losing-exec")
        losing.unlock("losing-exec", losing_owner)

        remote.gc()

        assert remote._store._exists(remote._cas_key(dag_ref.id()))
        assert losing._snapshot(losing._execution_key("losing-exec")) is None


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

        remote.upload_object_graph(dag_ref, source_db)
        remote._store._put(remote._cas_key(value.id()), remote._dump_cas_object(ScalarDatum("different")))

        with pytest.raises(ValueError, match="Remote CAS object identity mismatch"):
            remote.materialize_ref(dag_ref, target_db)


def test_deleting_ref_moves_original_payload_to_tombstone(tmp_path):
    with mock_aws():
        client = boto3.client("s3", region_name="us-east-1")
        client.create_bucket(Bucket="bucket")
        remote = Remote("s3://bucket/root", n_workers=2, client=client)
        source_db = make_db(tmp_path / "source-db")

        with source_db.tx() as txn:
            tree = txn.put(Tree(dags={}, tags={}))
            commit_ref = txn.put(Commit(parents=[], tree=tree, author="alice", message="snapshot"))

        branch_path = remote.put_ref(commit_ref, "branch", "main", source_db)
        original = json.dumps(remote._read_ref(branch_path), sort_keys=True)

        assert remote._del(branch_path) is True

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
