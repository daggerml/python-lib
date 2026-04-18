from __future__ import annotations

import json
import shlex
import subprocess
from typing import Any

from daggerml import Uri
from daggerml._internal.types import DmlRepoError, Runnable
from daggerml.contrib.adapters import AdapterBase
from daggerml.contrib.executor_state import ExecutionRecord, ExecutionState
from daggerml.contrib.executors._base import ExecutorBase


class SshExecutor(ExecutorBase):
    name = "ssh"
    adapter = "local"

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

    @staticmethod
    def _encode_value(value: Any) -> Any:
        if isinstance(value, Uri):
            return value.uri
        if isinstance(value, Runnable):
            return SshExecutor._encode_runnable(value)
        if isinstance(value, dict):
            return {k: SshExecutor._encode_value(v) for k, v in value.items()}
        if isinstance(value, list):
            return [SshExecutor._encode_value(v) for v in value]
        if isinstance(value, tuple):
            return [SshExecutor._encode_value(v) for v in value]
        return value

    @staticmethod
    def _encode_runnable(runnable: Runnable) -> dict[str, Any]:
        return {
            "target": runnable.target.uri,
            "adapter": runnable.adapter,
            "kwargs": SshExecutor._encode_value(runnable.kwargs),
            "sub": None if runnable.sub is None else SshExecutor._encode_runnable(runnable.sub),
        }

    @staticmethod
    def _child_cache_key(cache_key: str) -> str:
        return f"{cache_key}:ssh-child"

    @classmethod
    def _metadata(cls, *, runnable: Runnable, child_cache_key: str, remote: dict[str, str]) -> dict[str, Any]:
        if runnable.sub is None:
            raise DmlRepoError("ssh executor requires sub runnable metadata")
        return {
            cls.name: {
                "child_cache_key": child_cache_key,
                "env_files": cls._string_list("env_files", runnable.kwargs.get("env_files")),
                "flags": cls._string_list("flags", runnable.kwargs.get("flags")),
                "host": cls._host(runnable.kwargs),
                "remote_root": remote["root"],
                "sub_runnable": cls._encode_runnable(runnable.sub),
            }
        }

    @staticmethod
    def _terminal_child_state(child_cache_key: str) -> ExecutionRecord:
        child = ExecutionState(child_cache_key).get()
        if child is None:
            raise DmlRepoError(f"SSH nested execution missing child state for cache_key={child_cache_key!r}")
        if child["status"] not in {"succeeded", "failed"}:
            raise DmlRepoError(
                f"SSH nested execution returned terminal adapter output but child state is {child['status']!r}"
            )
        return child

    @staticmethod
    def _mark_parent_failed(cache_key: str, error: str) -> None:
        es = ExecutionState(cache_key)
        if es.lock():
            try:
                es.mark_failed(error)
            finally:
                es.unlock()

    @staticmethod
    def _project_child_terminal(*, cache_key: str, child_cache_key: str) -> None:
        parent = ExecutionState(cache_key)
        child = SshExecutor._terminal_child_state(child_cache_key)
        if not parent.lock():
            return
        try:
            if child["status"] == "succeeded":
                dag_id = child.get("dag_id")
                if not isinstance(dag_id, str) or not dag_id:
                    raise DmlRepoError("SSH nested execution succeeded without dag_id")
                parent.mark_succeeded(dag_id)
                return
            error = child.get("error")
            if not isinstance(error, str) or not error:
                error = "SSH nested execution failed without error"
            parent.mark_failed(error)
        finally:
            parent.unlock()

    @classmethod
    def _run_transport(
        cls,
        *,
        cache_key: str,
        child_cache_key: str,
        argv_ptr: str,
        host: str,
        flags: list[str],
        env_files: list[str],
        runnable: Runnable,
        remote: dict[str, str],
    ) -> None:
        cmd = ["ssh", *flags, host, cls._remote_command(env_files=env_files, adapter=runnable.adapter)]
        payload = AdapterBase._dump_payload(
            runnable=runnable,
            argv_ptr=argv_ptr,
            cache_key=child_cache_key,
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
            cls._mark_parent_failed(cache_key, error)
            return

        try:
            result = json.loads(stdout)
        except json.JSONDecodeError as e:
            cls._mark_parent_failed(cache_key, f"SSH nested adapter returned invalid JSON: {e}")
            return

        try:
            AdapterBase._validate_output(result)
        except DmlRepoError as e:
            cls._mark_parent_failed(cache_key, str(e))
            return

        if result["status"] in {"pending", "running"}:
            return

        try:
            cls._project_child_terminal(cache_key=cache_key, child_cache_key=child_cache_key)
        except DmlRepoError as e:
            cls._mark_parent_failed(cache_key, str(e))

    def start(
        self, *, cache_key: str, state: ExecutionRecord, runnable: Runnable, argv_ptr: str, remote: dict[str, str]
    ) -> None:
        if runnable is None or runnable.sub is None:
            raise DmlRepoError("ssh executor start requires runnable with sub runnable")
        child_cache_key = self._child_cache_key(cache_key)
        ExecutionState.upsert(child_cache_key, argv_ptr)
        es = ExecutionState(cache_key)
        try:
            assert es.lock()
            try:
                es.update_metadata(self._metadata(runnable=runnable, child_cache_key=child_cache_key, remote=remote))
            finally:
                es.unlock()
        except AssertionError as e:
            raise DmlRepoError(f"ssh executor failed to lock parent state for cache_key={cache_key!r}") from e

        self._run_transport(
            cache_key=cache_key,
            child_cache_key=child_cache_key,
            argv_ptr=argv_ptr,
            host=self._host(runnable.kwargs),
            flags=self._string_list("flags", runnable.kwargs.get("flags")),
            env_files=self._string_list("env_files", runnable.kwargs.get("env_files")),
            runnable=runnable.sub,
            remote=remote,
        )

    def poll(self, *, cache_key: str, state: ExecutionRecord) -> None:
        meta = (state.get("metadata") or {}).get(self.name, {})
        child_cache_key = meta.get("child_cache_key")
        remote_root = meta.get("remote_root")
        host = meta.get("host")
        flags = meta.get("flags")
        env_files = meta.get("env_files")
        sub_runnable = meta.get("sub_runnable")
        if (
            not isinstance(child_cache_key, str)
            or not isinstance(remote_root, str)
            or not isinstance(host, str)
            or not isinstance(flags, list)
            or not isinstance(env_files, list)
            or not isinstance(sub_runnable, dict)
        ):
            self._mark_parent_failed(cache_key, "ssh executor missing child metadata")
            return
        self._run_transport(
            cache_key=cache_key,
            child_cache_key=child_cache_key,
            argv_ptr=state["argv_ptr"],
            host=host,
            flags=self._string_list("flags", flags),
            env_files=self._string_list("env_files", env_files),
            runnable=AdapterBase._decode_runnable(sub_runnable),
            remote={"root": remote_root},
        )

    def cleanup(self, *, cache_key: str, state: ExecutionRecord) -> None:
        pass
