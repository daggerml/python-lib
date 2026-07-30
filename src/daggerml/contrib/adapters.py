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
from daggerml._core.exec_state import AdapterCancelResponse, AdapterInvokeResponse
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
    def send(cls, **kw) -> AdapterInvokeResponse | AdapterCancelResponse:
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
        payload["state"] = payload.get("state")
        while args.poll and payload.get("operation") == "invoke" and result.get("status") == "running":
            payload["state"] = result.get("state") or payload["state"]
            time.sleep(0.1)
            result = cls.send(**payload)
        cls._write_output(args.output, json.dumps(result))
        return 0


class LocalAdapter(AdapterBase):
    name = "local"
    executable = "dml-local-adapter"

    @classmethod
    def send(cls, **kw) -> AdapterInvokeResponse | AdapterCancelResponse:
        from daggerml.contrib.executors._base import get_executor

        return get_executor("local", kw["runnable"]["target"]["uri"]).handle(**kw)


class LambdaAdapter(AdapterBase):
    name = "lambda"
    executable = "dml-lambda-adapter"

    @classmethod
    def send(cls, **kw) -> AdapterInvokeResponse | AdapterCancelResponse:
        client = get_client("lambda")
        response = client.invoke(
            FunctionName=kw["runnable"]["target"]["uri"],
            InvocationType="RequestResponse",
            Payload=json.dumps(kw, separators=(",", ":"), sort_keys=True).encode("utf-8"),
        )
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
