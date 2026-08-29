from __future__ import annotations

import json
from dataclasses import asdict

from daggerml import Ref, Runnable, Uri
from daggerml.contrib.adapters import AdapterBase


def _runnable() -> Runnable:
    return Runnable(target=Uri("script"), kwargs={"image": Uri("s3://bucket/image.tar")}, adapter="dml-local-adapter")


def test_contrib_adapter_001__cli_passes_plain_payload_and_returns_raw_result(tmp_path):
    calls = []

    class RecordingAdapter(AdapterBase):
        @classmethod
        def send(cls, **kwargs):
            calls.append(kwargs)
            return {"status": "success", "error": None, "adapter_state": {}}

    payload = {
        "operation": "invoke",
        "runnable": asdict(_runnable()),
        "cache_key": "ck",
        "execution_id": "exec",
        "remote": {"root": "s3://bucket/root"},
        "scratch_uri": "s3://bucket/root/scratch",
        "adapter_state": {"job": "123"},
    }
    input_path = tmp_path / "input.json"
    output_path = tmp_path / "output.json"
    input_path.write_text(json.dumps(payload))

    assert RecordingAdapter.cli(["-i", str(input_path), "-o", str(output_path)]) == 0
    assert calls == [payload]
    assert json.loads(output_path.read_text()) == {
        "status": "success",
        "error": None,
        "adapter_state": {},
    }


def test_contrib_adapter_002__cli_polling_reuses_persisted_state_between_polls(tmp_path, monkeypatch):
    calls = []
    inspected = {}

    class PollingAdapter(AdapterBase):
        @classmethod
        def send(cls, **kwargs):
            calls.append(kwargs)
            if len(calls) == 1:
                return {"status": "retry", "error": None, "adapter_state": {"token": "abc"}}
            if len(calls) == 2:
                return {"status": "success", "error": None, "adapter_state": {"token": "abc"}}
            if len(calls) == 3:
                return {"status": "retry", "error": None, "adapter_state": {"cleanup": 1}}
            return {"status": "success", "error": None, "adapter_state": {"token": "abc"}}

    payload = json.dumps(
        {
            "operation": "invoke",
            "runnable": asdict(_runnable()),
            "cache_key": "ck",
            "execution_id": "exec",
            "remote": {"root": "s3://bucket/root"},
            "scratch_uri": "s3://bucket/root/scratch",
            "adapter_state": None,
        }
    )
    input_path = tmp_path / "input.json"
    output_path = tmp_path / "output.json"
    input_path.write_text(payload)
    class Runtime:
        def read_execution_record(self, execution):
            inspected["execution"] = execution
            return {"state": {"result_ref": "dag:result"}}

    class Dml:
        def __init__(self, *, remote_root):
            inspected["remote_root"] = remote_root
            self.runtime = Runtime()

    monkeypatch.setattr("daggerml.contrib.adapters.Dml", Dml)
    assert PollingAdapter.cli(["--poll", "-i", str(input_path), "-o", str(output_path)]) == 0
    persisted = json.loads(output_path.read_text())
    assert persisted["status"] == "success"
    assert inspected == {"remote_root": "s3://bucket/root", "execution": Ref("index:exec")}
    assert calls[1]["adapter_state"] == {"token": "abc"}
    assert calls[2]["operation"] == "cleanup"
    assert calls[2]["result_ref"] == "dag:result"
    assert calls[3]["operation"] == "cleanup"
    assert calls[3]["adapter_state"] == {"cleanup": 1}


def test_contrib_adapter_003__cli_supports_s3_input_and_output(monkeypatch):
    calls = []
    writes = {}

    class RecordingAdapter(AdapterBase):
        @classmethod
        def send(cls, **kwargs):
            calls.append(kwargs)
            return {"status": "success", "error": None, "adapter_state": {}}

    payload = {
        "operation": "invoke",
        "runnable": asdict(_runnable()),
        "cache_key": "ck",
        "execution_id": "exec",
        "remote": {"root": "s3://bucket/root"},
        "scratch_uri": "s3://bucket/root/scratch",
        "adapter_state": None,
    }

    class FakeStore:
        def get(self, uri):
            assert uri == "s3://bucket/input.json"
            return json.dumps(payload).encode("utf-8")

    class FakeClient:
        def put_object(self, **kwargs):
            writes.update(kwargs)

    monkeypatch.setattr("daggerml.contrib.adapters.S3Store", lambda: FakeStore())
    monkeypatch.setattr("daggerml.contrib.adapters.get_client", lambda service: FakeClient())

    assert RecordingAdapter.cli(["-i", "s3://bucket/input.json", "-o", "s3://bucket/output.json"]) == 0
    assert calls == [payload]
    assert writes == {
        "Bucket": "bucket",
        "Key": "output.json",
        "Body": json.dumps({"status": "success", "error": None, "adapter_state": {}}).encode("utf-8"),
        "ContentType": "application/json",
    }


def test_contrib_adapter_004__lambda_throttling_returns_resumable_retry(monkeypatch):
    class Throttled(Exception):
        response = {
            "Error": {"Code": "TooManyRequestsException"},
            "ResponseMetadata": {"HTTPHeaders": {"retry-after": "2"}},
        }

    class Client:
        def invoke(self, **kwargs):
            raise Throttled()

    monkeypatch.setattr("daggerml.contrib.adapters.get_client", lambda service: Client())
    from daggerml.contrib.adapters import LambdaAdapter

    result = LambdaAdapter.send(
        operation="invoke",
        runnable=asdict(_runnable()),
        cache_key="ck",
        execution_id="exec",
        remote={"root": "s3://bucket/root"},
        scratch_uri="s3://bucket/root/scratch",
        adapter_state=None,
    )

    assert result == {"status": "retry", "error": None, "adapter_state": {}, "retry_after_ms": 2000}
