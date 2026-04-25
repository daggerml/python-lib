from __future__ import annotations

import json
import shlex
import subprocess
from typing import Any, TypedDict, cast

from daggerml import Uri
from daggerml._internal.types import DmlRepoError, Runnable
from daggerml.contrib.adapters import AdapterBase
from daggerml.contrib.api import is_node_like
from daggerml.contrib.executors._base import ExecutorBase

SshExecKwargs = TypedDict("SshExecutorKwargs", {"host": str, "flags": list[str], "env_files": list[str]})


def _is_node_string_list(value: Any) -> bool:
    return is_node_like(value) or isinstance(value, list) and all(isinstance(item, str) and item for item in value)


class SshExecutor(ExecutorBase):
    name = "ssh"
    adapter = "local"

    @classmethod
    def handle(
        cls,
        *,
        cache_key: str,
        execution_id: str,
        state: dict[str, Any] | None,
        runnable: Runnable,
        argv_ptr: str,
        remote: dict[str, str],
    ) -> dict[str, Any]:
        if runnable is None or runnable.sub is None:
            raise DmlRepoError("ssh executor handle requires runnable with sub runnable")
        kw = cls._validate_kw(runnable.kwargs)
        cmd = [
            "ssh",
            *kw["flags"],
            kw["host"],
            cls._remote_command(env_files=kw["env_files"], adapter=runnable.sub.adapter),
        ]
        payload = AdapterBase._dump_payload(
            runnable=runnable.sub,
            argv_ptr=argv_ptr,
            cache_key=cache_key,
            execution_id=execution_id,
            remote=remote,
            state=state,
        )
        proc = subprocess.run(cmd, input=payload, capture_output=True, check=False)
        stdout = proc.stdout.decode("utf-8", errors="replace").strip()
        stderr = proc.stderr.decode("utf-8", errors="replace").strip()
        if proc.returncode != 0:
            error = f"SSH command failed ({proc.returncode})"
            if stderr:
                error = f"{error}: {stderr}"
            elif stdout:
                error = f"{error}: {stdout}"
            return {"status": "failed", "error": error}
        try:
            result = json.loads(stdout)
        except json.JSONDecodeError as e:
            return {"status": "failed", "error": f"SSH nested adapter returned invalid JSON: {e}"}
        if not isinstance(result, dict) or result.get("status") not in {"succeeded", "failed", "running"}:
            return {"status": "failed", "error": f"SSH nested adapter returned unexpected result: {result}"}
        return result

    @staticmethod
    def _validate_kw(kw: dict) -> SshExecKwargs:
        if not isinstance(kw, dict):
            raise DmlRepoError("ssh executor kwargs must be a dict")
        if set(kw.keys()) > {"env_files", "flags", "host"}:
            raise DmlRepoError("ssh executor kwargs only supports keys: env_files, flags, host")
        host = cast(str, kw.get("host"))
        if not (is_node_like(host) or (isinstance(host, str) and host)):
            raise DmlRepoError("ssh executor requires non-empty host")
        kw["flags"] = flags = cast(list[str], kw.get("flags") or [])
        if not _is_node_string_list(flags):
            raise DmlRepoError("ssh executor flags must be a list of non-empty strings")
        kw["env_files"] = env_files = cast(list[str], kw.get("env_files") or [])
        if not _is_node_string_list(env_files):
            raise DmlRepoError("ssh executor env_files must be a list of non-empty strings")
        return SshExecKwargs(host=host, flags=flags, env_files=env_files)

    @classmethod
    def resolve_runnable(cls, uri, kwargs, sub):
        if sub is None:
            raise DmlRepoError("ssh executor requires sub runnable")
        unknown = sorted(set(kwargs.keys()) - {"env_files", "flags", "host"})
        if unknown:
            raise DmlRepoError(f"Unknown ssh executor kwargs: {', '.join(unknown)}")
        return Runnable(
            target=Uri("ssh"),
            kwargs=dict(cls._validate_kw(kwargs)),
            sub=sub,
            adapter="dml-local-adapter",
        )

    @staticmethod
    def _remote_command(*, env_files: list[str], adapter: str) -> str:
        parts = ["set -e"]
        parts.extend(f". {shlex.quote(path)}" for path in env_files)
        parts.append(f"exec {shlex.quote(adapter)} --poll -i - -o -")
        return "; ".join(parts)
