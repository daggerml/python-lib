"""Portable runtime restoration against real LMDB projects and Moto S3 coordination."""

import time
from urllib.parse import urlparse

import boto3
import pytest

import daggerml.api as api
from daggerml import Dml, Runnable, Uri
from daggerml._core.dml import _exec_state, _remote_ops
from daggerml._core.types import ArgvNode, DictDatum, ListDatum, RunnableDatum, ScalarDatum, UriDatum
from daggerml.api import DmlRepoError
from daggerml.contrib.api import funkify


@funkify(uri="script")
def checkpoint_training(dag, snapshot_uri):
    from urllib.parse import urlparse

    import boto3

    dag.put({"epoch": 2}, name="checkpoint")
    dag.freeze("preemption")
    uri = urlparse(snapshot_uri.value())
    boto3.client("s3").put_object(Bucket=uri.netloc, Key=uri.path.lstrip("/"),
                                Body=dag.export_snapshot().encode())


def test_script_checkpoint_resumes_without_relaunch_and_returns_normal_result(runtime_world, monkeypatch):
    from daggerml.contrib.codecs import DelayedActionCodec

    monkeypatch.setattr(api, "_codecs", [(1, 1, DelayedActionCodec()), (0, 2, api.NodeCodec()),
                                        (0, 3, api.MiscPyTypeCodec()), (0, 4, api.ProjectionCodec())])
    monkeypatch.setattr(api, "_plugins_loaded", True)
    world = runtime_world
    caller = api.new(dml=world.publisher)
    fn = caller.put(checkpoint_training)
    uri = f"{world.root}/training-snapshot.json"
    arg = caller.put(uri)
    argv = [fn.ref, arg.ref]
    client = boto3.client("s3")
    parsed = urlparse(uri)
    resumed = None
    try:
        assert caller._start_fn(argv) is None
        deadline = time.monotonic() + 30
        snapshot = None
        while time.monotonic() < deadline:
            try:
                snapshot = client.get_object(Bucket=parsed.netloc, Key=parsed.path.lstrip("/"))["Body"].read().decode()
                break
            except client.exceptions.NoSuchKey:
                time.sleep(0.05)
        assert snapshot is not None, "worker did not publish its snapshot"
        # The original caller observes suspension, not the old worker's exit.
        assert caller._start_fn(argv) is None
        home = world.home / "resumed-worker"
        home.mkdir()
        target = Dml.init(str(home), remote_root=world.root, user="replacement")
        resumed = api.resume(snapshot=snapshot, dml=target)
        assert resumed["checkpoint"].value() == {"epoch": 2}
        resumed.commit(resumed.put({"epoch": 3}))
        deadline = time.monotonic() + 30
        result = None
        while time.monotonic() < deadline:
            result = caller._start_fn(argv)
            if result is not None:
                break
            time.sleep(0.05)
        assert result is not None
        assert world.publisher.dag.describe_node(result)["dag"] == resumed.ref
        caller.commit(result)
    finally:
        # Release/tear down spawned workers even when an assertion fails.
        if caller.token is not None:
            world.publisher.runtime.cancel(caller.token, max_retries=1)


@pytest.mark.parametrize("execution_runtime", [False, True])
def test_snapshot_cross_project_resumption_and_cache_publication(runtime_world, execution_runtime):
    world = runtime_world
    source = world.publisher
    cache_key = None
    if execution_runtime:
        # Persist real argv data without launching an external worker: resumption
        # is application-driven, not an automatic executor relaunch.
        def arguments(txn):
            fn = txn.put(RunnableDatum(target=txn.put(UriDatum("script")),
                                       sub=None, kwargs=txn.put(DictDatum({})), adapter="dml-local-adapter"))
            return txn.put(ArgvNode(value=txn.put(ListDatum([fn, txn.put(ScalarDatum(3))]))))

        argv = source._db.write_with_growth(arguments)
        with source._db.tx(readonly=True) as txn:
            cache_key = txn.get(argv).cache_key(txn)
        _remote_ops(source).upload_object_graph(argv, source._db)
        state = _exec_state(source, cache_key)
        execution_id, owner, _ = state.reserve_execution(argv)
        state._create_cache(cache_key, execution_id)
        state.unlock(execution_id, owner)
        draft = api.new(cache_key=cache_key, execution_id=execution_id, dml=source, tags=["training"])
    else:
        draft = api.new(dml=source, tags=["training"])
        execution_id = draft.token.id()
        state = _exec_state(source)
    draft.put({"epoch": 2, "loss": 0.5}, name="checkpoint")
    draft.freeze("preemption")
    snapshot = draft.export_snapshot()
    before = state.read_execution_record(execution_id)
    assert before["state"]["lifecycle"] == "suspended"

    target_home = world.home / "replacement"
    target_home.mkdir()
    target = Dml.init(str(target_home), user="replacement", remote_root=world.root)
    resumed = api.resume(snapshot=snapshot, dml=target, name="finished", message="continue checkpoint")
    assert resumed["checkpoint"].value() == {"epoch": 2, "loss": 0.5}
    assert resumed.tags == ["training"]
    if execution_runtime:
        assert resumed.argv.value() == [Runnable(Uri("script"), kwargs={}, adapter="dml-local-adapter"), 3]
    resumed.commit(resumed.put({"epoch": 3}, name="final"))
    with pytest.raises(DmlRepoError, match="stale|not suspended"):
        draft.unfreeze()
    after = state.read_execution_record(execution_id)
    assert after["metadata"] == before["metadata"]
    assert after["driver"] == before["driver"]
    assert after["state"]["result_ref"] == resumed.ref.to
    if cache_key:
        owner = state.acquire(execution_id)
        state._finalize_runtime_result(execution_id, owner)
        state.unlock(execution_id, owner)
        assert state.get_cached_result(cache_key, target._db) == resumed.ref
