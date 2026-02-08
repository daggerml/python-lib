from __future__ import annotations

import argparse
import json
import os
import sys
import time
from pathlib import Path
from typing import Any

from daggerml._internal.types import DmlRepoError, Runnable, Uri
from daggerml.contrib.executor_registry import get_executor
from daggerml.contrib.executor_state import lock_from_comms


class AdapterBase:
    name = ""

    @classmethod
    def resolve_runnable(cls, uri, kwargs, sub):
        spec = get_executor(cls.name, uri)
        resolved = spec.resolve_runnable(uri, kwargs, sub)
        if not isinstance(resolved, Runnable):
            raise DmlRepoError(f"Executor '{uri}' resolve_runnable must return Runnable")
        return resolved

    @classmethod
    def send(cls, *, runnable: Runnable, argv_ptr: str, cache_key: str, remote: dict[str, str]):
        raise NotImplementedError("Adapter send method is not implemented")

    @classmethod
    def _dump_payload(
        cls,
        *,
        runnable: Runnable,
        argv_ptr: str,
        cache_key: str,
        remote: dict[str, str],
        comms: dict[str, Any] | None = None,
    ) -> bytes:
        def _encode(value: Any) -> Any:
            if isinstance(value, Runnable):
                return {
                    "target": value.target.uri,
                    "kwargs": {k: _encode(v) for k, v in value.kwargs.items()},
                    "adapter": value.adapter,
                    "sub": None if value.sub is None else _encode(value.sub),
                }
            if isinstance(value, Uri):
                return value.uri
            if isinstance(value, dict):
                return {k: _encode(v) for k, v in value.items()}
            if isinstance(value, list):
                return [_encode(v) for v in value]
            if isinstance(value, tuple):
                return [_encode(v) for v in value]
            return value

        payload: dict[str, Any] = {
            "runnable": _encode(runnable),
            "argv_ptr": argv_ptr,
            "cache_key": cache_key,
            "remote": _encode(remote),
        }
        if comms is not None:
            payload["comms"] = _encode(comms)
        return json.dumps(payload).encode("utf-8")

    @staticmethod
    def _decode_runnable(value: Any) -> Runnable:
        if isinstance(value, Runnable):
            return value
        if not isinstance(value, dict):
            raise DmlRepoError("Adapter runnable payload must be a dict")
        target = value.get("target")
        kwargs = value.get("kwargs", {})
        adapter = value.get("adapter")
        sub = value.get("sub")
        if not isinstance(target, str):
            raise DmlRepoError("Adapter runnable target must be a string")
        if not isinstance(kwargs, dict):
            raise DmlRepoError("Adapter runnable kwargs must be a dict")
        if not isinstance(adapter, str):
            raise DmlRepoError("Adapter runnable adapter must be a string")
        return Runnable(
            target=Uri(target),
            kwargs=kwargs,
            adapter=adapter,
            sub=(None if sub is None else AdapterBase._decode_runnable(sub)),
        )

    @classmethod
    def _parse_payload(cls, payload: dict) -> tuple[str, str, Runnable, dict[str, str], dict[str, Any] | None]:
        argv_ptr = payload["argv_ptr"]
        cache_key = payload["cache_key"]
        remote = payload["remote"]
        comms = payload.get("comms")
        if not isinstance(argv_ptr, str):
            raise DmlRepoError("Adapter payload argv_ptr must be a string")
        if not isinstance(cache_key, str):
            raise DmlRepoError("Adapter payload cache_key must be a string")
        if not isinstance(remote, dict):
            raise DmlRepoError("Adapter payload remote must be a dict")
        if comms is not None and not isinstance(comms, dict):
            raise DmlRepoError("Adapter payload comms must be a dict when provided")
        return argv_ptr, cache_key, cls._decode_runnable(payload["runnable"]), remote, comms

    @classmethod
    def _load_current_state(cls, *, runnable: Runnable, cache_key: str) -> dict[str, Any] | None:
        spec = get_executor(cls.name, runnable.target.uri)
        return spec.state_class.new(cache_key).get()

    @classmethod
    def _report_parent_comms(
        cls,
        *,
        comms: dict[str, Any] | None,
        runnable: Runnable,
        cache_key: str,
        result: dict[str, Any],
    ) -> None:
        if comms is None:
            return
        current = cls._load_current_state(runnable=runnable, cache_key=cache_key)
        with lock_from_comms(cache_key, comms) as state:
            if state is None:
                return
            if current is None:
                record = state.init_record(status=result["status"], error=result["error"])
            else:
                record = state.update_status(
                    status=current["status"],
                    error=current["error"],
                    owner_executor=current["owner_executor"],
                    owner_instance=current["owner_instance"],
                    heartbeat_ts=current["heartbeat_ts"],
                    lease_expires_ts=current["lease_expires_ts"],
                )
            state.update(record)

    @staticmethod
    def _validate_output(result):
        if not isinstance(result, dict):
            raise DmlRepoError("Adapter output must be a dict")
        expected = {"status", "error"}
        if set(result.keys()) != expected:
            raise DmlRepoError("Adapter output keys must be exactly: status, error")
        status = result.get("status")
        if status not in {"pending", "running", "succeeded", "failed", "canceled"}:
            raise DmlRepoError("Adapter output status must be one of pending|running|succeeded|failed|canceled")
        error = result.get("error")
        if status == "failed":
            if error is None:
                raise DmlRepoError("Adapter output failed requires error")
        else:
            if error is not None:
                raise DmlRepoError("Adapter output running/pending/succeeded/canceled requires error=None")
        return result

    @classmethod
    def _read_input(cls, input_path: str) -> str:
        if input_path == "-":
            return sys.stdin.read()
        return Path(input_path).read_text()

    @classmethod
    def _write_output(cls, output_path: str, data: str) -> None:
        if output_path == "-":
            sys.stdout.write(data)
            if not data.endswith("\n"):
                sys.stdout.write("\n")
            sys.stdout.flush()
            return
        Path(output_path).write_text(data)

    @classmethod
    def cli(cls, argv: list[str] | None = None) -> int:
        parser = argparse.ArgumentParser(description=f"{cls.__name__} CLI")
        parser.add_argument("-i", "--input", default="-")
        parser.add_argument("-o", "--output", default="-")
        parser.add_argument("--poll", action="store_true")
        args = parser.parse_args(argv)

        raw = cls._read_input(args.input)
        payload = json.loads(raw)
        argv_ptr, cache_key, runnable, remote, comms = cls._parse_payload(payload)
        result = cls.send(runnable=runnable, argv_ptr=argv_ptr, cache_key=cache_key, remote=remote)
        cls._report_parent_comms(comms=comms, runnable=runnable, cache_key=cache_key, result=result)
        while args.poll and result.get("status") not in {"succeeded", "failed", "canceled"}:
            time.sleep(0.05)
            result = cls.send(runnable=runnable, argv_ptr=argv_ptr, cache_key=cache_key, remote=remote)
            cls._report_parent_comms(comms=comms, runnable=runnable, cache_key=cache_key, result=result)
        cls._write_output(args.output, json.dumps(result))
        return 0


class LocalAdapter(AdapterBase):
    name = "local"
    executable = "dml-local-adapter"

    @classmethod
    def send(cls, *, runnable: Runnable, argv_ptr: str, cache_key: str, remote: dict[str, str]):
        spec = get_executor("local", runnable.target.uri)
        state_class = spec.state_class
        with state_class.lock(cache_key) as state:
            if state is None:
                return {"status": "running", "error": None}
            current = state.get()
            if current is None:
                result = spec.start(
                    runnable=runnable,
                    argv_ptr=argv_ptr,
                    cache_key=cache_key,
                    remote=remote,
                    state=state,
                )
                if result.get("status") in {"succeeded", "failed", "canceled"}:
                    spec.gc(state=state)
            else:
                status = current.get("status")
                if status in {"succeeded", "failed", "canceled"}:
                    result = {"status": status, "error": current.get("error")}
                    spec.gc(state=state)
                else:
                    result = spec.poll(state=state)
                    if result.get("status") in {"succeeded", "failed", "canceled"}:
                        spec.gc(state=state)
        return cls._validate_output(result)


class LambdaAdapter(AdapterBase):
    name = "lambda"
    executable = "dml-lambda-adapter"

    @classmethod
    def send(cls, *, runnable: Runnable, argv_ptr: str, cache_key: str, remote: dict[str, str]):
        function_name = os.getenv("DML_LAMBDA_FUNCTION")
        if not function_name:
            raise DmlRepoError("Lambda adapter requires DML_LAMBDA_FUNCTION")
        try:
            import boto3
        except Exception as e:
            raise DmlRepoError(f"Lambda adapter requires boto3: {e}") from e
        client = boto3.client("lambda")
        response = client.invoke(
            FunctionName=function_name,
            InvocationType="RequestResponse",
            Payload=cls._dump_payload(runnable=runnable, argv_ptr=argv_ptr, cache_key=cache_key, remote=remote),
        )
        stream = response.get("Payload")
        if stream is None:
            raise DmlRepoError("Lambda adapter invoke response missing Payload")
        raw = stream.read()
        body = raw.decode("utf-8") if isinstance(raw, (bytes, bytearray)) else str(raw)
        try:
            result = json.loads(body) if body else {}
        except json.JSONDecodeError as e:
            raise DmlRepoError(f"Lambda adapter response payload must be JSON: {e}") from e
        return cls._validate_output(result)
