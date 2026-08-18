"""Slow Moto-backed lifecycle coverage for real script ``@funkify`` executions.

These tests deliberately use independently initialized repositories and the public
Dml cache/runtime APIs.  Their worker functions are self-contained because the
script executor serializes function source rather than test-module globals.
"""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from queue import Queue
from threading import Barrier, Event
from time import monotonic
from uuid import uuid4

import pytest

import daggerml.api as api
from daggerml import Dml
from daggerml.contrib.api import funkify
from daggerml.contrib.codecs import DelayedActionCodec


@funkify(adapter="local", uri="script")
def record_single_body_run(dag, marker_key, value):
    """Record this script-body entry as an S3 object version and return ``value``."""
    import boto3

    boto3.client("s3").put_object(Bucket="test-bucket", Key=marker_key.value(), Body=b"entered")
    return dag.put(value.value(), name="result")


@funkify(adapter="local", uri="script")
def wait_for_release(dag, started_key, release_key, value):
    """Publish a durable readiness marker, then wait for an explicit S3 release."""
    from time import sleep

    import boto3
    from botocore.exceptions import ClientError

    client = boto3.client("s3")
    client.put_object(Bucket="test-bucket", Key=started_key.value(), Body=b"started")
    while True:
        try:
            client.head_object(Bucket="test-bucket", Key=release_key.value())
        except ClientError as exc:
            if exc.response["Error"]["Code"] not in {"404", "NoSuchKey", "NoSuchBucket"}:
                raise
            sleep(0.02)
        else:
            return dag.put(value.value(), name="result")


def _configure_real_runtime(monkeypatch, home, *, user="tester"):
    """Create one isolated real repository with the script-runtime test codecs."""
    monkeypatch.setenv("DML_DEFAULT_DB_MAP_SIZE_MAX", str(64 * 1024 * 1024))
    monkeypatch.setattr(
        api,
        "_codecs",
        [(1, 1, DelayedActionCodec()), (0, 2, api.NodeCodec()), (0, 3, api.MiscPyTypeCodec())],
    )
    return Dml.init(str(home), user=user, remote_root="s3://test-bucket/test-prefix")


def _wait_for_object(s3_client, key: str, *, timeout=20) -> None:
    """Wait for a worker's durable readiness marker without a launch-time race."""
    deadline = monotonic() + timeout
    retry = Event()
    while monotonic() < deadline:
        try:
            s3_client.head_object(Bucket="test-bucket", Key=key)
            return
        except s3_client.exceptions.ClientError as exc:
            if exc.response["Error"]["Code"] not in {"404", "NoSuchKey"}:
                raise
            retry.wait(0.02)
    raise AssertionError(f"worker did not publish readiness marker: {key}")


def _cache_key_for_result(dml, result) -> str:
    """Read the exact normalized cache key from the completed function DAG."""
    cache_key = dml.dag.describe(result.context().ref)["cache_key"]
    assert isinstance(cache_key, str)
    return cache_key


def _object_version_count(s3_client, key: str) -> int:
    """Return all non-delete S3 versions for an execution-body marker."""
    response = s3_client.list_object_versions(Bucket="test-bucket", Prefix=key)
    return sum(version["Key"] == key for version in response.get("Versions", []))


@pytest.mark.slow
def test_contrib_int_009__concurrent_equivalent_funks_share_one_execution_and_body(
    tmp_path, monkeypatch, remote_env, s3_bucket, s3_client
):
    """Twelve simultaneous calls to one real funk converge on one execution.

    The callers intentionally share one DML repository, DAG, and stored funk—the
    topology used by concurrent callers in one process.  The barrier is immediately
    before the identical invocation, so setup cannot serialize the cache race.
    Bucket versioning makes script-body entry observable: one marker version proves
    one body launch, not merely equivalent eventual results.
    """
    del remote_env, s3_bucket
    s3_client.put_bucket_versioning(Bucket="test-bucket", VersioningConfiguration={"Status": "Enabled"})
    marker_key = "lifecycle-markers/concurrent-body"
    callers = 12
    barrier = Barrier(callers)
    home = tmp_path / "shared-caller"
    home.mkdir()
    dml = _configure_real_runtime(monkeypatch, home)
    dag = api.new("concurrent", dml=dml)
    fn = dag.put(record_single_body_run, name="fn")

    def invoke(_: int):
        # Synchronize at the public funk call, after all shared setup is complete.
        barrier.wait(timeout=60)
        return fn(marker_key, 41, sleep=lambda: 0, timeout=60_000)

    with ThreadPoolExecutor(max_workers=callers) as pool:
        outcomes = list(pool.map(invoke, range(callers)))

    execution_dags = {result.context().ref for result in outcomes}
    cache_keys = {_cache_key_for_result(dml, result) for result in outcomes}
    cache_descriptions = [dml.cache.describe(next(iter(cache_keys))) for _ in outcomes]

    assert [result.value() for result in outcomes] == [41] * callers
    assert len(execution_dags) == 1
    assert len(cache_keys) == 1
    assert len({description["execution"] for description in cache_descriptions if description is not None}) == 1
    assert _object_version_count(s3_client, marker_key) == 1


def _spawned_execution(dml, caller_index):
    """Return the sole cache-backed child execution of a live caller runtime."""
    graph = dml.runtime.describe_graph(caller_index)
    spawned = graph["nodes"][caller_index.id()]["spawned"]
    assert len(spawned) == 1
    return api.Ref(f"index:{spawned[0]}")


@pytest.mark.slow
@pytest.mark.xfail(
    reason="Cancellation leaves D0's exclusive live leaf in cancel-ready instead of canceled.",
)
def test_contrib_int_010__canceling_one_dag_preserves_shared_dependency_for_another(
    tmp_path, monkeypatch, remote_env, s3_bucket, s3_client
):
    """Cancel D0's exclusive leaf while D1 keeps its shared leaf alive.

    One user/project/home owns D0 -> {f0, f1} and D1 -> {f1, f2}.  Every script
    leaf publishes a UUID-derived S3 readiness object then waits for its own release
    object.  This makes cancellation occur only after the complete shared graph is
    live, without timing-based synchronization.
    """
    del remote_env, s3_bucket
    home = tmp_path / "one-user-one-home"
    home.mkdir()
    dml = _configure_real_runtime(monkeypatch, home, user="user")
    d0, d1 = api.new("D0", dml=dml), api.new("D1", dml=dml)
    f0 = d0.put(wait_for_release, name="f0")
    f1_d0 = d0.put(wait_for_release, name="f1")
    f1_d1 = d1.put(wait_for_release, name="f1")
    f2 = d1.put(wait_for_release, name="f2")
    leaves = {
        "f0": (f0, "f0"),
        "f1-d0": (f1_d0, "f1"),
        "f1-d1": (f1_d1, "f1"),
        "f2": (f2, "f2"),
    }

    run_id = uuid4().hex

    def invoke(name, fn, leaf):
        started = f"lifecycle-markers/{run_id}/{leaf}-started"
        release = f"lifecycle-markers/{run_id}/{leaf}-release"
        try:
            return name, fn(started, release, leaf, sleep=lambda: 0, timeout=60_000)
        except BaseException as exc:
            return name, exc

    with ThreadPoolExecutor(max_workers=4) as pool:
        futures = [pool.submit(invoke, name, *item) for name, item in leaves.items()]
        for leaf in ("f0", "f1", "f2"):
            _wait_for_object(s3_client, f"lifecycle-markers/{run_id}/{leaf}-started")

        # D0 owns f0 and shares f1 with D1.  Canceling D0 must not cancel f1.
        dml.runtime.cancel(d0.token, mode="full")
        dml.runtime.cancel(d0.token, mode="drive")

        d0_graph = dml.runtime.describe_graph(d0.token)
        d1_graph = dml.runtime.describe_graph(d1.token)
        d0_spawned = set(d0_graph["nodes"][d0.token.id()]["spawned"])
        d1_spawned = set(d1_graph["nodes"][d1.token.id()]["spawned"])
        assert len(d0_spawned) == 2
        assert len(d1_spawned) == 2
        shared = d0_spawned & d1_spawned
        assert len(shared) == 1
        exclusive_d0 = d0_spawned - shared
        assert len(exclusive_d0) == 1

        for execution_id in exclusive_d0:
            execution = api.Ref(f"index:{execution_id}")
            assert dml.runtime.read_execution_record(execution)["lifecycle"] == "canceled"
            assert execution not in {item["id"] for item in dml.runtime.list()}

        # D1 still owns f1, so both surviving leaves are explicitly released.
        for leaf in ("f1", "f2"):
            s3_client.put_object(
                Bucket="test-bucket",
                Key=f"lifecycle-markers/{run_id}/{leaf}-release",
                Body=b"release",
            )
        outcomes = dict(future.result(timeout=30) for future in futures)

    assert isinstance(outcomes["f0"], BaseException)
    # D0's caller sees its shared dependency as canceled, while D1's caller keeps
    # the same execution alive and receives its eventual result.
    assert isinstance(outcomes["f1-d0"], BaseException)
    assert not isinstance(outcomes["f1-d1"], BaseException)
    assert not isinstance(outcomes["f2"], BaseException)
    shared_execution = api.Ref(f"index:{shared.pop()}")
    assert dml.runtime.read_execution_record(shared_execution)["lifecycle"] == "succeeded"
    assert outcomes["f1-d1"].value() == "f1"
    assert outcomes["f2"].value() == "f2"


@pytest.mark.slow
def test_contrib_int_011__public_invalidation_removes_completed_funk_cache_and_reexecutes(
    tmp_path, monkeypatch, remote_env, s3_bucket, s3_client
):
    """Invalidate a completed execution by public execution ref, then recompute it."""
    del remote_env, s3_bucket
    s3_client.put_bucket_versioning(Bucket="test-bucket", VersioningConfiguration={"Status": "Enabled"})
    marker_key = "lifecycle-markers/invalidation-body"
    first_home = tmp_path / "first"
    first_home.mkdir()
    first_dml = _configure_real_runtime(monkeypatch, first_home)
    first_dag = api.new("invalidation-first", dml=first_dml)
    first_result = first_dag.put(record_single_body_run, name="fn")(marker_key, 7, sleep=lambda: 0, timeout=30_000)
    cache_key = _cache_key_for_result(first_dml, first_result)
    first_description = first_dml.cache.describe(cache_key)
    assert first_description is not None
    assert first_description["dag"] == first_result.context().ref
    assert _object_version_count(s3_client, marker_key) == 1

    invalidated = first_dml.cache.invalidate(first_description["execution"])
    assert [item["execution_id"] for item in invalidated["invalidations"]] == [first_description["execution"].id()]
    assert first_dml.cache.describe(cache_key) is None
    assert first_dml.cache.get(cache_key) is None

    second_home = tmp_path / "second"
    second_home.mkdir()
    second_dml = _configure_real_runtime(monkeypatch, second_home, user="second")
    second_dag = api.new("invalidation-second", dml=second_dml)
    second_result = second_dag.put(record_single_body_run, name="fn")(marker_key, 7, sleep=lambda: 0, timeout=30_000)
    second_description = second_dml.cache.describe(cache_key)

    assert second_result.value() == 7
    assert second_description is not None
    assert second_description["execution"] != first_description["execution"]
    assert _object_version_count(s3_client, marker_key) == 2
