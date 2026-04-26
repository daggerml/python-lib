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
    def send(
        cls,
        *,
        runnable: Runnable,
        argv_ptr: str,
        cache_key: str,
        execution_id: str,
        remote: dict[str, str],
        state: dict[str, Any] | None,
    ):
        raise NotImplementedError("Adapter send method is not implemented")

    @classmethod
    def _dump_payload(
        cls,
        *,
        runnable: Runnable,
        argv_ptr: str,
        cache_key: str,
        execution_id: str,
        remote: dict[str, str],
        state: dict[str, Any] | None,
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
            "execution_id": execution_id,
            "remote": _encode(remote),
            "state": _encode(state),
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
    def _parse_payload(cls, payload: dict) -> tuple[str, str, str, Runnable, dict[str, str], dict[str, Any] | None]:
        argv_ptr = payload["argv_ptr"]
        cache_key = payload["cache_key"]
        execution_id = payload["execution_id"]
        remote = payload["remote"]
        state = payload.get("state")
        if not isinstance(argv_ptr, str):
            raise DmlRepoError("Adapter payload argv_ptr must be a string")
        if not isinstance(cache_key, str):
            raise DmlRepoError("Adapter payload cache_key must be a string")
        if not isinstance(execution_id, str):
            raise DmlRepoError("Adapter payload execution_id must be a string")
        if not isinstance(remote, dict):
            raise DmlRepoError("Adapter payload remote must be a dict")
        if state is not None and not isinstance(state, dict):
            raise DmlRepoError("Adapter payload state must be a dict or null")
        return argv_ptr, cache_key, execution_id, cls._decode_runnable(payload["runnable"]), remote, state

    @staticmethod
    def _validate_output(result):
        if not isinstance(result, dict):
            raise DmlRepoError("Adapter output must be a dict")
        status = result.get("status")
        if status not in {"running", "succeeded", "failed"}:
            raise DmlRepoError("Adapter output status must be one of running|succeeded|failed")
        allowed_keys = {"status", "error"}
        if status == "succeeded":
            allowed_keys.add("dag_id")
        elif status == "running":
            allowed_keys.add("state")
        extra = set(result.keys()) - allowed_keys
        if extra:
            raise DmlRepoError(f"Adapter output has unexpected keys: {', '.join(sorted(extra))}")
        error = result.get("error")
        if status == "failed":
            if error is None:
                raise DmlRepoError("Adapter output failed requires error")
        elif status == "running":
            if error is not None:
                raise DmlRepoError("Adapter output running requires error=None")
            state = result.get("state")
            if not isinstance(state, dict):
                raise DmlRepoError("Adapter output running requires state")
        else:
            if error is not None:
                raise DmlRepoError("Adapter output succeeded requires error=None")
            dag_id = result.get("dag_id")
            if not isinstance(dag_id, str) or not dag_id:
                raise DmlRepoError("Adapter output succeeded requires dag_id")
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
        if is_s3_uri(output_path):
            from urllib.parse import urlparse

            import boto3
            parsed = urlparse(output_path)
            bucket = parsed.netloc
            key = parsed.path.lstrip("/")
            boto3.client("s3").put_object(
                Bucket=bucket,
                Key=key,
                Body=data.encode("utf-8"),
                ContentType="application/json",
            )
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
        argv_ptr, cache_key, execution_id, runnable, remote, state = cls._parse_payload(payload)
        result = cls.send(
            runnable=runnable,
            argv_ptr=argv_ptr,
            cache_key=cache_key,
            execution_id=execution_id,
            remote=remote,
            state=state,
        )
        persisted_state = state
        while args.poll and result.get("status") not in {"succeeded", "failed"}:
            if persisted_state is None:
                persisted_state = result.get("state")
            time.sleep(0.05)
            result = cls.send(
                runnable=runnable,
                argv_ptr=argv_ptr,
                cache_key=cache_key,
                execution_id=execution_id,
                remote=remote,
                state=persisted_state,
            )
        cls._write_output(args.output, json.dumps(result))
        return 0


class LocalAdapter(AdapterBase):
    name = "local"
    executable = "dml-local-adapter"

    @classmethod
    def send(
        cls,
        *,
        runnable: Runnable,
        argv_ptr: str,
        cache_key: str,
        execution_id: str,
        remote: dict[str, str],
        state: dict[str, Any] | None,
    ):
        spec = get_executor("local", runnable.target.uri)
        if not hasattr(spec, "handle"):
            raise DmlRepoError(f"Executor '{runnable.target.uri}' does not support handle()")
        result = spec.handle(
            cache_key=cache_key,
            execution_id=execution_id,
            state=state,
            runnable=runnable,
            argv_ptr=argv_ptr,
            remote=remote,
        )
        return cls._validate_output(result)


class LambdaAdapter(AdapterBase):
    name = "lambda"
    executable = "dml-lambda-adapter"

    @classmethod
    def send(
        cls,
        *,
        runnable: Runnable,
        argv_ptr: str,
        cache_key: str,
        execution_id: str,
        remote: dict[str, str],
        state: dict[str, Any] | None,
    ):
        client = get_client("lambda")
        response = client.invoke(
            FunctionName=runnable.target.uri,
            InvocationType="RequestResponse",
            Payload=cls._dump_payload(
                runnable=runnable,
                argv_ptr=argv_ptr,
                cache_key=cache_key,
                execution_id=execution_id,
                remote=remote,
                state=state,
            ),
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
