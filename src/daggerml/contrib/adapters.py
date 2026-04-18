from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path
from typing import Any

from daggerml._internal.types import DmlRepoError, Runnable, Uri
from daggerml.contrib.executor_registry import get_executor
from daggerml.contrib.s3 import S3Store, is_s3_uri
from daggerml.util import get_client


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
    def _parse_payload(cls, payload: dict) -> tuple[str, str, Runnable, dict[str, str]]:
        argv_ptr = payload["argv_ptr"]
        cache_key = payload["cache_key"]
        remote = payload["remote"]
        if not isinstance(argv_ptr, str):
            raise DmlRepoError("Adapter payload argv_ptr must be a string")
        if not isinstance(cache_key, str):
            raise DmlRepoError("Adapter payload cache_key must be a string")
        if not isinstance(remote, dict):
            raise DmlRepoError("Adapter payload remote must be a dict")
        return argv_ptr, cache_key, cls._decode_runnable(payload["runnable"]), remote

    @staticmethod
    def _validate_output(result):
        if not isinstance(result, dict):
            raise DmlRepoError("Adapter output must be a dict")
        expected = {"status", "error"}
        if set(result.keys()) != expected:
            raise DmlRepoError("Adapter output keys must be exactly: status, error")
        status = result.get("status")
        if status not in {"pending", "running", "succeeded", "failed"}:
            raise DmlRepoError("Adapter output status must be one of pending|running|succeeded|failed")
        error = result.get("error")
        if status == "failed":
            if error is None:
                raise DmlRepoError("Adapter output failed requires error")
        else:
            if error is not None:
                raise DmlRepoError("Adapter output running/pending/succeeded requires error=None")
        return result

    @classmethod
    def _read_input(cls, input_path: str) -> str:
        if input_path == "-":
            return sys.stdin.read()
        if is_s3_uri(input_path):
            return S3Store().get(input_path).decode("utf-8")
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
        argv_ptr, cache_key, runnable, remote = cls._parse_payload(payload)
        result = cls.send(runnable=runnable, argv_ptr=argv_ptr, cache_key=cache_key, remote=remote)
        while args.poll and result.get("status") not in {"succeeded", "failed"}:
            time.sleep(0.05)
            result = cls.send(runnable=runnable, argv_ptr=argv_ptr, cache_key=cache_key, remote=remote)
        cls._write_output(args.output, json.dumps(result))
        return 0


class LocalAdapter(AdapterBase):
    name = "local"
    executable = "dml-local-adapter"

    @classmethod
    def send(cls, *, runnable: Runnable, argv_ptr: str, cache_key: str, remote: dict[str, str]):
        spec = get_executor("local", runnable.target.uri)
        if not hasattr(spec, "handle"):
            raise DmlRepoError(f"Executor '{runnable.target.uri}' does not support handle()")
        result = spec.handle(cache_key=cache_key, runnable=runnable, argv_ptr=argv_ptr, remote=remote)
        return cls._validate_output(result)


class LambdaAdapter(AdapterBase):
    name = "lambda"
    executable = "dml-lambda-adapter"

    @classmethod
    def send(cls, *, runnable: Runnable, argv_ptr: str, cache_key: str, remote: dict[str, str]):
        client = get_client("lambda")
        response = client.invoke(
            FunctionName=runnable.target.uri,
            InvocationType="RequestResponse",
            Payload=cls._dump_payload(runnable=runnable, argv_ptr=argv_ptr, cache_key=cache_key, remote=remote),
        )
        stream = response.get("Payload")
        if stream is None:
            raise DmlRepoError("Lambda adapter invoke response missing Payload")
        body = stream.read().decode("utf-8")
        try:
            result = json.loads(body) if body else {}
        except json.JSONDecodeError as e:
            raise DmlRepoError(f"Lambda adapter response payload must be JSON: {e}") from e
        return cls._validate_output(result)
