from __future__ import annotations

import os
import time
from concurrent.futures import ThreadPoolExecutor
from unittest.mock import patch

import boto3
import pytest

from daggerml._internal.types import DmlRepoError, Runnable, Uri
from daggerml.contrib.executor_state import ExecutionState
from daggerml.contrib.executors._base import ExecutorBase

TABLE_NAME = "test-executor-base"


@pytest.fixture(scope="module")
def _dynamo_server():
    with patch.dict(os.environ):
        for key in list(os.environ.keys()):
            if key.startswith("AWS_"):
                del os.environ[key]
        from moto.server import ThreadedMotoServer

        server = ThreadedMotoServer(port=0, verbose=False)
        server.start()
        host, port = server.get_host_and_port()
        try:
            yield f"http://{host}:{port}"
        finally:
            server.stop()


@pytest.fixture(autouse=True)
def dynamo_env(_dynamo_server):
    with patch.dict(
        os.environ,
        {
            "AWS_ACCESS_KEY_ID": "test",
            "AWS_SECRET_ACCESS_KEY": "test",
            "AWS_REGION": "us-east-1",
            "AWS_DEFAULT_REGION": "us-east-1",
            "AWS_ENDPOINT_URL": _dynamo_server,
            "DML_DYNAMODB_TABLE": TABLE_NAME,
        },
    ):
        boto3.setup_default_session()
        client = boto3.client("dynamodb", endpoint_url=_dynamo_server)
        try:
            client.create_table(
                TableName=TABLE_NAME,
                KeySchema=[{"AttributeName": "cache_key", "KeyType": "HASH"}],
                AttributeDefinitions=[{"AttributeName": "cache_key", "AttributeType": "S"}],
                BillingMode="PAY_PER_REQUEST",
            )
        except client.exceptions.ResourceInUseException:
            scan = client.scan(TableName=TABLE_NAME)
            for item in scan.get("Items", []):
                client.delete_item(TableName=TABLE_NAME, Key={"cache_key": item["cache_key"]})
        yield


def _runnable() -> Runnable:
    return Runnable(target=Uri("test"), kwargs={}, adapter="test-adapter")


def _remote() -> dict[str, str]:
    return {"root": "s3://test/prefix"}


class MockExecutor(ExecutorBase):
    name = "mock"
    adapter = "local"
    calls: list[str] = []

    def start(self, *, cache_key, state, runnable, argv_ptr, remote):
        MockExecutor.calls.append("start")

    def poll(self, *, cache_key, state):
        MockExecutor.calls.append("poll")

    def cleanup(self, *, cache_key, state):
        MockExecutor.calls.append("cleanup")


class TerminalStartExecutor(MockExecutor):
    def start(self, *, cache_key, state, runnable, argv_ptr, remote):
        MockExecutor.calls.append("start")
        es = ExecutionState(cache_key)
        assert es.lock()
        try:
            assert es.mark_succeeded("dag-start")
        finally:
            es.unlock()


class SlowStartExecutor(MockExecutor):
    def start(self, *, cache_key, state, runnable, argv_ptr, remote):
        MockExecutor.calls.append("start")
        time.sleep(0.05)


@pytest.fixture(autouse=True)
def reset_calls():
    MockExecutor.calls = []
    yield


class TestHandle:
    def test_start_called_on_pending(self):
        ExecutionState.upsert("hb-1", "ptr")
        result = MockExecutor.handle(cache_key="hb-1", runnable=_runnable(), argv_ptr="ptr", remote=_remote())
        assert "start" in MockExecutor.calls
        assert result["status"] == "running"
        record = ExecutionState("hb-1").get()
        assert record is not None
        assert record["status"] == "running"

    def test_poll_called_on_running(self):
        ExecutionState.upsert("hb-2", "ptr")
        es = ExecutionState("hb-2")
        assert es.lock()
        assert es.mark_running()
        es.unlock()

        result = MockExecutor.handle(cache_key="hb-2", runnable=_runnable(), argv_ptr="ptr", remote=_remote())
        assert "poll" in MockExecutor.calls
        assert result["status"] == "running"

    def test_cleanup_called_on_succeeded(self):
        ExecutionState.upsert("hb-3", "ptr")
        es = ExecutionState("hb-3")
        assert es.lock()
        assert es.mark_running()
        assert es.mark_succeeded("dag-1")
        es.unlock()

        result = MockExecutor.handle(cache_key="hb-3", runnable=_runnable(), argv_ptr="ptr", remote=_remote())
        assert "cleanup" in MockExecutor.calls
        assert result["status"] == "succeeded"

    def test_cleanup_called_on_failed(self):
        ExecutionState.upsert("hb-4", "ptr")
        es = ExecutionState("hb-4")
        assert es.lock()
        assert es.mark_running()
        assert es.mark_failed("oops")
        es.unlock()

        result = MockExecutor.handle(cache_key="hb-4", runnable=_runnable(), argv_ptr="ptr", remote=_remote())
        assert "cleanup" in MockExecutor.calls
        assert result["status"] == "failed"
        assert result["error"] == "oops"

    def test_done_state_is_rejected_from_handle(self):
        ExecutionState.upsert("hb-5", "ptr")
        es = ExecutionState("hb-5")
        assert es.lock()
        try:
            assert es.mark_running()
            assert es.mark_succeeded("dag-done")
            assert es.mark_done()
        finally:
            es.unlock()
        MockExecutor.calls.clear()

        with pytest.raises(DmlRepoError, match=r"unexpected execution status 'done'"):
            MockExecutor.handle(cache_key="hb-5", runnable=_runnable(), argv_ptr="ptr", remote=_remote())
        assert MockExecutor.calls == []

    def test_start_cleanup_runs_when_start_finishes_terminal(self):
        ExecutionState.upsert("hb-start-terminal", "ptr")

        result = TerminalStartExecutor.handle(
            cache_key="hb-start-terminal",
            runnable=_runnable(),
            argv_ptr="ptr",
            remote=_remote(),
        )

        assert MockExecutor.calls == ["start", "cleanup"]
        assert result["status"] == "succeeded"

    def test_pending_claim_prevents_duplicate_launch(self):
        ExecutionState.upsert("hb-race", "ptr")

        with ThreadPoolExecutor(max_workers=2) as pool:
            results = list(
                pool.map(
                    lambda _: SlowStartExecutor.handle(
                        cache_key="hb-race",
                        runnable=_runnable(),
                        argv_ptr="ptr",
                        remote=_remote(),
                    ),
                    range(2),
                )
            )

        assert MockExecutor.calls.count("start") == 1
        assert [result["status"] for result in results] == ["running", "running"]

    def test_terminal_state_remains_for_start_fn_finalization(self):
        ExecutionState.upsert("hb-6", "ptr")
        es = ExecutionState("hb-6")
        assert es.lock()
        assert es.mark_running()
        assert es.mark_succeeded("dag-x")
        es.unlock()

        MockExecutor.handle(cache_key="hb-6", runnable=_runnable(), argv_ptr="ptr", remote=_remote())

        rec = ExecutionState("hb-6").get()
        assert rec is not None
        assert rec["status"] == "succeeded"
