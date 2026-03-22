from __future__ import annotations

import json
import shlex
import subprocess
from dataclasses import dataclass
from typing import Any

from daggerml import Uri
from daggerml._internal.types import DmlRepoError, Runnable
from daggerml.contrib.adapters import AdapterBase
from daggerml.contrib.executor_state import LocalState
from daggerml.contrib.executors._base import ExecutorBase


@dataclass
class SshExecutor(ExecutorBase):
    runnable: Runnable | None = None
    name = "ssh"
    adapter = "local"
    state_class = LocalState

    @staticmethod
    def _string_list(name: str, value: Any) -> list[str]:
        if value is None:
            return []
        if not isinstance(value, list) or not all(isinstance(item, str) and item for item in value):
            raise DmlRepoError(f"ssh executor {name} must be a list[str]")
        return list(value)

    @staticmethod
    def _host(kwargs: dict[str, Any]) -> str:
        host = kwargs.get("host")
        if not isinstance(host, str) or not host:
            raise DmlRepoError("ssh executor requires non-empty host")
        return host

    @classmethod
    def resolve_runnable(cls, uri, kwargs, sub):
        if sub is None:
            raise DmlRepoError("ssh executor requires sub runnable")
        unknown = sorted(set(kwargs.keys()) - {"env_files", "flags", "host"})
        if unknown:
            raise DmlRepoError(f"Unknown ssh executor kwargs: {', '.join(unknown)}")
        return Runnable(
            target=Uri("ssh"),
            kwargs={
                "host": cls._host(kwargs),
                "flags": cls._string_list("flags", kwargs.get("flags")),
                "env_files": cls._string_list("env_files", kwargs.get("env_files")),
            },
            sub=sub,
            adapter="dml-local-adapter",
        )

    @staticmethod
    def _remote_command(*, env_files: list[str], adapter: str) -> str:
        parts = ["set -e"]
        parts.extend(f". {shlex.quote(path)}" for path in env_files)
        parts.append(f"exec {shlex.quote(adapter)} -i - -o -")
        return "; ".join(parts)

    @classmethod
    def start(cls, *, runnable, argv_ptr, cache_key, remote, state):
        if runnable is None or runnable.sub is None:
            raise DmlRepoError("ssh executor start requires runnable with sub runnable")
        host = cls._host(runnable.kwargs)
        flags = cls._string_list("flags", runnable.kwargs.get("flags"))
        env_files = cls._string_list("env_files", runnable.kwargs.get("env_files"))
        cmd = ["ssh", *flags, host, cls._remote_command(env_files=env_files, adapter=runnable.sub.adapter)]
        payload = AdapterBase._dump_payload(
            runnable=runnable.sub,
            argv_ptr=argv_ptr,
            cache_key=cache_key,
            remote=remote,
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
        try:
            return AdapterBase._validate_output(result)
        except DmlRepoError as e:
            return {"status": "failed", "error": str(e)}

    @classmethod
    def poll(cls, *, state):
        return {"status": "pending", "error": None}

    @classmethod
    def gc(cls, *, state):
        return None
