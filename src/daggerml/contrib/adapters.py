from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path
from threading import Lock
from typing import Any
from urllib.parse import urlparse
from warnings import warn

from daggerml import Runnable
from daggerml._core.exec_state import (
    AdapterCancelResponse,
    AdapterCleanupResponse,
    AdapterInvokeResponse,
    ExecutionState,
)
from daggerml.api import DmlRepoError, _entry_points
from daggerml.contrib.s3 import S3Store, is_s3_uri
from daggerml.util import get_client


class AdapterBase:
    name = ""

    @classmethod
    def resolve_runnable(cls, uri, kwargs, sub) -> Runnable:
        from daggerml.contrib.executors._base import get_executor

        return get_executor(cls.name, uri).resolve_runnable(uri, kwargs, sub)

    @classmethod
    def send(cls, **kw) -> AdapterInvokeResponse | AdapterCleanupResponse | AdapterCancelResponse:
        raise NotImplementedError("Adapter send method is not implemented")

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
            parsed = urlparse(output_path)
            bucket = parsed.netloc
            key = parsed.path.lstrip("/")
            get_client("s3").put_object(
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
        # FIXME: `--poll` make `cancel` difficult. We should allow for coordination between caller and this loop.
        args = parser.parse_args(argv)
        raw = cls._read_input(args.input)
        payload = json.loads(raw)
        result = cls.send(**payload)
        while args.poll and payload.get("operation") == "invoke" and result.get("status") == "retry":
            state = result.get("adapter_state")
            if not isinstance(state, dict):
                raise DmlRepoError("Retry adapter response requires object adapter_state")
            payload["adapter_state"] = state
            time.sleep(0.1)
            result = cls.send(**payload)
        if args.poll and payload.get("operation") == "invoke" and result.get("status") == "success":
            record = ExecutionState.from_execution_id(
                payload["execution_id"], root_uri=payload["remote"]["root"], n_workers=1
            ).read_execution_record(payload["execution_id"])
            result_ref = record["state"]["result_ref"]
            if result_ref is None:
                raise DmlRepoError("Successful nested invoke did not publish a result")
            cleanup_payload = {**payload, "operation": "cleanup", "result_ref": result_ref}
            result = cls.send(**cleanup_payload)
            while result.get("status") == "retry":
                state = result.get("adapter_state")
                if not isinstance(state, dict):
                    raise DmlRepoError("Retry adapter response requires object adapter_state")
                cleanup_payload["adapter_state"] = state
                time.sleep(0.1)
                result = cls.send(**cleanup_payload)
        cls._write_output(args.output, json.dumps(result))
        return 0


class LocalAdapter(AdapterBase):
    name = "local"
    executable = "dml-local-adapter"

    @classmethod
    def send(cls, **kw) -> AdapterInvokeResponse | AdapterCleanupResponse | AdapterCancelResponse:
        from daggerml.contrib.executors._base import get_executor

        return get_executor("local", kw["runnable"]["target"]["uri"]).handle(**kw)


class LambdaAdapter(AdapterBase):
    name = "lambda"
    executable = "dml-lambda-adapter"

    @classmethod
    def send(cls, **kw) -> AdapterInvokeResponse | AdapterCleanupResponse | AdapterCancelResponse:
        client = get_client("lambda")
        try:
            response = client.invoke(
                FunctionName=kw["runnable"]["target"]["uri"],
                InvocationType="RequestResponse",
                Payload=json.dumps(kw, separators=(",", ":"), sort_keys=True).encode("utf-8"),
            )
        except Exception as exc:
            code = getattr(exc, "response", {}).get("Error", {}).get("Code")
            if code not in {"TooManyRequestsException", "ThrottlingException"}:
                raise
            headers = getattr(exc, "response", {}).get("ResponseMetadata", {}).get("HTTPHeaders", {})
            try:
                retry_after_ms = max(0, int(float(headers.get("retry-after")) * 1000))
            except (TypeError, ValueError):
                retry_after_ms = None
            result: AdapterInvokeResponse = {
                "status": "retry",
                "error": None,
                "adapter_state": kw.get("adapter_state") if isinstance(kw.get("adapter_state"), dict) else {},
            }
            if retry_after_ms is not None:
                result["retry_after_ms"] = retry_after_ms
            return result
        stream = response.get("Payload")
        if stream is None:
            raise DmlRepoError("Lambda adapter invoke response missing Payload")
        return json.loads(stream.read().decode("utf-8"))


################################################################################
############################### Adapter registry ###############################
################################################################################
ADAPTER_ENTRYPOINT_GROUP = "daggerml.contrib.adapters"

_LOCK = Lock()
_ADAPTER_SPECS: dict[str, str] = {}
_PLUGINS_LOADED = False


def load_adapter_plugins() -> None:
    global _PLUGINS_LOADED
    if _PLUGINS_LOADED:
        return
    with _LOCK:
        if _PLUGINS_LOADED:
            return
        for ep in _entry_points(ADAPTER_ENTRYPOINT_GROUP):
            try:
                loaded = ep.load()
                if loaded.name in _ADAPTER_SPECS:
                    # warn about duplicate adapter registration but allow the last one to win
                    warn(
                        f"Adapter: '{loaded.name}' is overwriting existing '{ep.name} ({ep.value})'",
                        stacklevel=2,
                    )
                _ADAPTER_SPECS[loaded.name] = loaded
            except Exception as e:
                raise DmlRepoError(f"Adapter plugin '{ep.name} ({ep.value})' failed: {e}") from e
        _PLUGINS_LOADED = True


def get_adapter(name: str) -> Any:
    load_adapter_plugins()
    spec = _ADAPTER_SPECS.get(name)
    if spec is None:
        raise DmlRepoError(f"Adapter '{name}' is not registered")
    return spec


def list_adapters() -> list[str]:
    load_adapter_plugins()
    return sorted(_ADAPTER_SPECS.keys())
