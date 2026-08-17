from __future__ import annotations

import json
from dataclasses import asdict

from daggerml import Runnable, Uri
from daggerml.contrib.adapters import AdapterBase


def _runnable() -> Runnable:
    return Runnable(target=Uri("script"), kwargs={"image": Uri("s3://bucket/image.tar")}, adapter="dml-local-adapter")


def test_contrib_adapter_001__cli_passes_plain_payload_and_returns_raw_result(tmp_path):
    calls = []

    class RecordingAdapter(AdapterBase):
        @classmethod
        def send(cls, **kwargs):
            calls.append(kwargs)
            return {"status": "succeeded", "error": None, "adapter_state": {}, "dag_id": "d" * 64}

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
        "status": "succeeded",
        "error": None,
        "adapter_state": {},
        "dag_id": "d" * 64,
    }


def test_contrib_adapter_002__cli_polling_reuses_persisted_state_between_polls(tmp_path):
    calls = []

    class PollingAdapter(AdapterBase):
        @classmethod
        def send(cls, **kwargs):
            calls.append(kwargs)
            if len(calls) == 1:
                return {"status": "running", "error": None, "adapter_state": {"token": "abc"}, "dag_id": None}
            return {"status": "succeeded", "error": None, "adapter_state": {"token": "abc"}, "dag_id": "d" * 64}

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
    assert PollingAdapter.cli(["--poll", "-i", str(input_path), "-o", str(output_path)]) == 0
    persisted = json.loads(output_path.read_text())
    assert persisted["status"] == "succeeded"
    assert calls[1]["adapter_state"] == {"token": "abc"}


def test_contrib_adapter_003__cli_supports_s3_input_and_output(monkeypatch):
    calls = []
    writes = {}

    class RecordingAdapter(AdapterBase):
        @classmethod
        def send(cls, **kwargs):
            calls.append(kwargs)
            return {"status": "succeeded", "error": None, "adapter_state": {}, "dag_id": "d" * 64}

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
        "Body": json.dumps({"status": "succeeded", "error": None, "adapter_state": {}, "dag_id": "d" * 64}).encode(
            "utf-8"
        ),
        "ContentType": "application/json",
    }
