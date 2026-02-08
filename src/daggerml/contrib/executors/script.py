from __future__ import annotations

import argparse
import ast
import inspect
import json
import logging
import os
import shutil
import signal
import subprocess
import sys
import tempfile
import time
from contextlib import chdir
from pathlib import Path
from tempfile import TemporaryDirectory
from textwrap import dedent
from typing import Any, cast

import daggerml as dml
from daggerml._internal.types import DmlRepoError, Runnable, Uri
from daggerml.contrib.executor_state import LocalState
from daggerml.contrib.executors._base import ExecutorBase
from daggerml.contrib.s3 import S3Store, is_s3_uri

logger = logging.getLogger(__name__)


META_KEY = "__dml_script_exec__"


class ScriptExecutor(ExecutorBase):
    name = "script"
    adapter = "local"
    state_class = LocalState
    LEASE_SECONDS = 30.0

    def __init__(self, runnable: Runnable | None = None, argv_ptr: str | None = None):
        self.runnable = runnable
        self.argv_ptr = argv_ptr

    @staticmethod
    def _script_kwargs(kwargs: dict[str, Any]) -> tuple[dict[str, Any], str]:
        allowed = {"fn", "prepop", "extra_objs", "extra_lines"}
        unknown = sorted(set(kwargs.keys()) - allowed)
        if unknown:
            bad = ", ".join(unknown)
            raise DmlRepoError(f"Unknown script executor kwargs: {bad}")

        fn = kwargs.get("fn")
        if not callable(fn):
            raise DmlRepoError("script resolve_runnable requires callable fn")

        prepop = kwargs.get("prepop", {})
        if not isinstance(prepop, dict):
            raise DmlRepoError("script prepop must be a dict")

        extra_objs = list(kwargs.get("extra_objs", []))
        if not isinstance(extra_objs, list):
            raise DmlRepoError(f"script extra_objs must be a list, not {type(extra_objs).__name__}")

        extra_lines = list(kwargs.get("extra_lines", []))
        if not isinstance(extra_lines, list) or not all(isinstance(x, str) for x in extra_lines):
            raise DmlRepoError("script extra_lines must be a list[str]")

        call_kwargs = {}
        params = list(inspect.signature(fn).parameters.values())
        if not params or params[0].name != "dag":
            raise DmlRepoError("script fn must include first 'dag' parameter")

        for p in params[1:]:
            has_default = p.default is not inspect._empty
            if has_default:
                call_kwargs[p.name] = p.default

        script = ScriptExecutor._render_script(fn, extra_objs=extra_objs, extra_lines=extra_lines)

        return {
            META_KEY: {
                "prepop": prepop,
                "fn_name": fn.__name__,
            },
            **call_kwargs,
        }, script

    @staticmethod
    def _proc_exists(pid: int) -> bool:
        try:
            os.kill(pid, 0)
            return True
        except ProcessLookupError:
            return False
        except PermissionError:
            return True

    @staticmethod
    def _strip_funkify_decorators(source: str) -> str:
        module = ast.parse(source)
        for node in module.body:
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                node.decorator_list = []
        return ast.unparse(module).strip()

    @staticmethod
    def _render_script(fn, *, extra_objs: list[Any], extra_lines: list[str]) -> str:
        chunks: list[str] = []
        for obj in [*extra_objs, fn]:
            try:
                raw = dedent(inspect.getsource(inspect.unwrap(obj))).strip()
                chunks.append(ScriptExecutor._strip_funkify_decorators(raw))
            except (OSError, TypeError) as e:
                raise DmlRepoError(f"Failed to serialize object source: {e}") from e

        if extra_lines:
            chunks.extend(extra_lines)

        script = "\n".join(["\n\n".join(chunks), "\n"])
        try:
            mod = ast.parse(script)
        except SyntaxError as e:
            raise DmlRepoError(f"Generated script is not valid Python: {e}") from e

        if not any(isinstance(n, ast.FunctionDef) and n.name == fn.__name__ for n in mod.body):
            raise DmlRepoError(f"Function '{fn.__name__}' is not globally defined in generated script")

        return script

    @classmethod
    def resolve_runnable(cls, uri, kwargs, sub):
        if sub is not None:
            raise DmlRepoError("script executor does not accept sub runnable")
        resolved_kwargs, script = cls._script_kwargs(dict(kwargs))
        script_uri = S3Store().put(data=script.encode("utf-8"), suffix=".py")
        meta = dict(resolved_kwargs[META_KEY])
        meta["script_uri"] = script_uri.uri
        return Runnable(
            target=Uri("script"),
            kwargs={**resolved_kwargs, META_KEY: meta},
            sub=sub,
            adapter="dml-local-adapter",
        )

    @staticmethod
    def _runtime_inputs(*, runnable, argv_ptr) -> tuple[str, str, dict[str, Any]]:
        meta = runnable.kwargs.get(META_KEY)
        if not isinstance(meta, dict):
            raise DmlRepoError("script runnable missing script metadata")
        script_uri = meta.get("script_uri")
        if not isinstance(script_uri, str) or not is_s3_uri(script_uri):
            raise DmlRepoError("script runnable script_uri must be an s3:// URI")

        fn_name = meta.get("fn_name")
        if not isinstance(fn_name, str) or not fn_name:
            raise DmlRepoError("script runnable missing fn_name")

        call_kwargs = {k: v for k, v in runnable.kwargs.items() if k != META_KEY}

        if not isinstance(argv_ptr, str):
            raise DmlRepoError("script run requires argv_ptr string")
        return script_uri, fn_name, call_kwargs

    @classmethod
    def start(cls, *, runnable, argv_ptr, cache_key, remote, state=None):
        return cls(runnable=runnable, argv_ptr=argv_ptr)._start(cache_key=cache_key, remote=remote, state=state)

    def _start(self, *, cache_key, remote, state=None):
        if state is None:
            raise DmlRepoError("script start requires locked state")
        if self.runnable is None or self.argv_ptr is None:
            raise DmlRepoError("script start requires runnable and argv_ptr")
        _script_uri, _fn_name, _call_kwargs = self._runtime_inputs(runnable=self.runnable, argv_ptr=self.argv_ptr)
        record = state.get()
        if record is not None:
            status = record.get("status")
            if status in {"succeeded", "failed", "pending", "running", "canceled"}:
                return {"status": status, "error": record.get("error")}

        workdir = tempfile.mkdtemp(prefix=f"dml-script-{cache_key[:8]}-")
        payload_path = Path(workdir) / "supervisor-input.json"
        result_path = Path(workdir) / "result.json"
        stdout_path = Path(workdir) / "stdout.log"
        stderr_path = Path(workdir) / "stderr.log"
        payload = {
            "version": 1,
            "cache_key": cache_key,
            "cmd": ["python", "-m", "daggerml.contrib.executors.script", self.argv_ptr],
            "remote": remote,
            "comms": {"kind": "local", "spec": {}},
            "env": {
                "DML_REMOTE_ROOT": remote["root"],
                "DML_REMOTE_CACHE": remote["cache"],
                "DML_CACHE_KEY": cache_key,
            },
        }
        payload_path.write_text(json.dumps(payload, separators=(",", ":"), sort_keys=True))
        with stdout_path.open("w") as stdout_f, stderr_path.open("w") as stderr_f:
            proc = subprocess.Popen(
                [
                    "python",
                    "-m",
                    "daggerml.contrib.supervisor",
                    "-i",
                    str(payload_path),
                    "-o",
                    str(result_path),
                ],
                stdout=stdout_f,
                stderr=stderr_f,
                start_new_session=True,
                close_fds=True,
            )
        now = time.time()
        initial = state.init_record(
            status="running",
            owner_executor=self.name,
            owner_instance=f"supervisor:{proc.pid}",
            heartbeat_ts=now,
            lease_expires_ts=now + self.LEASE_SECONDS,
        )
        created = state.put_if_absent(initial)
        if created:
            with_meta = state.set_executor_metadata(
                executor_id=self.name,
                data={
                    "pid": proc.pid,
                    "workdir": workdir,
                    "result_path": str(result_path),
                    "stdout_path": str(stdout_path),
                    "stderr_path": str(stderr_path),
                },
            )
            state.update(with_meta)
        if not created:
            try:
                os.killpg(proc.pid, signal.SIGTERM)
            except ProcessLookupError:
                pass
            except PermissionError:
                pass
        return {"status": "running", "error": None}

    @classmethod
    def poll(cls, *, state=None):
        return cls()._poll(state=state)

    def _poll(self, *, state=None):
        if state is None:
            raise DmlRepoError("script poll requires locked state")
        record = state.get()
        if record is None:
            return {"status": "pending", "error": None}

        status = record.get("status")
        if status in {"succeeded", "failed", "canceled"}:
            return {"status": status, "error": record.get("error")}
        metadata = record.get("metadata")
        script_meta = metadata.get(self.name) if isinstance(metadata, dict) else None
        pid = script_meta.get("pid") if isinstance(script_meta, dict) else None
        stale_at = record.get("lease_expires_ts")
        if isinstance(stale_at, (int, float)) and stale_at < time.time() and isinstance(pid, int):
            if self._proc_exists(pid):
                try:
                    os.killpg(pid, signal.SIGTERM)
                except ProcessLookupError:
                    pass
                except PermissionError:
                    pass
                return {"status": "failed", "error": "Script supervisor heartbeat stale"}
        result_path = script_meta.get("result_path") if isinstance(script_meta, dict) else None
        if isinstance(pid, int) and not self._proc_exists(pid):
            if isinstance(result_path, str) and Path(result_path).exists():
                result = json.loads(Path(result_path).read_text())
                if isinstance(result, dict):
                    return {"status": result.get("status"), "error": result.get("error")}
            return {"status": "failed", "error": "Script supervisor exited without result"}
        return {"status": "running", "error": None}

    @classmethod
    def kill(cls, *, state=None):
        return cls()._kill(state=state)

    def _kill(self, *, state=None):
        if state is None:
            raise DmlRepoError("script kill requires locked state")
        record = state.get()
        if record is None:
            return {"status": "canceled", "error": None}

        status = record.get("status")
        if status in {"succeeded", "failed", "canceled"}:
            return {"status": status, "error": record.get("error")}

        metadata = record.get("metadata")
        script_meta = metadata.get(self.name) if isinstance(metadata, dict) else None
        pid = script_meta.get("pid") if isinstance(script_meta, dict) else None
        if isinstance(pid, int):
            try:
                os.killpg(pid, signal.SIGTERM)
            except ProcessLookupError:
                pass
            except PermissionError:
                pass

        canceled = state.update_status(
            status="canceled",
            error=None,
            owner_executor=cast(str | None, record.get("owner_executor")),
            owner_instance=cast(str | None, record.get("owner_instance")),
            heartbeat_ts=cast(float | None, record.get("heartbeat_ts")),
            lease_expires_ts=None,
        )
        state.update(canceled)
        return {"status": "canceled", "error": None}

    @classmethod
    def gc(cls, *, state=None):
        return cls()._gc(state=state)

    def _gc(self, *, state=None):
        if state is None:
            raise DmlRepoError("script gc requires locked state")
        record = state.get()
        if record is None:
            return None
        metadata = record.get("metadata")
        script_meta = metadata.get(self.name) if isinstance(metadata, dict) else None
        workdir = script_meta.get("workdir") if isinstance(script_meta, dict) else None
        if isinstance(workdir, str):
            shutil.rmtree(workdir, ignore_errors=True)
        return None


def _terminal_runnable(root: Runnable) -> Runnable:
    current = root
    while current.sub is not None:
        if not isinstance(current.sub, Runnable):
            raise DmlRepoError(f"script worker runnable.sub must be Runnable, got: {type(current.sub).__name__}")
        current = current.sub
    return current


def run_payload(argv_ptr: str) -> dict[str, Any]:
    # IMPORTANT:
    # This worker path is intentionally NOT a defensive validation boundary.
    # `run_payload` is only called by `main`, `main` is only called from this
    # module's CLI entrypoint, and that entrypoint is only launched by
    # `ScriptExecutor` with payloads that contrib itself constructed.
    # `ScriptExecutor.start` launches the supervisor, and the supervisor then
    # launches this worker with the payload/env it prepared. Anything the
    # supervisor is responsible for setting up can be assumed to exist here.
    # Treat all inputs here as trusted internal runtime data.
    # Do not add routine shape/type validation here unless a new external
    # trust boundary is introduced.
    namespace: dict[str, Any] = {"logger": logging.getLogger("daggerml.contrib.script")}

    def runit(dag):
        runnable_node, *arg_nodes = dag.argv
        runnable = _terminal_runnable(cast(Runnable, runnable_node.value()))
        metadata = cast(dict[str, Any], runnable.kwargs.pop(META_KEY))
        script_uri = cast(str, metadata["script_uri"])
        script = store.get(script_uri).decode("utf-8")
        fn_name = cast(str, metadata["fn_name"])
        call_kwargs = {k: dag.put(v, name=f"dml.kw:{k}") for k, v in runnable.kwargs.items()}
        prepop = cast(dict[str, Any], metadata.get("prepop", {}))
        for key, value in prepop.items():
            dag.put(value, name=key)
        exec(script, namespace)
        fn = namespace.get(fn_name)
        output = fn(dag, *arg_nodes, **call_kwargs)
        if dag.ref is None:
            dag.commit(output)

    with dml.temporary() as dml_instance:
        try:
            store = S3Store()
            dag = dml_instance.new(argv_ptr=argv_ptr)
        except Exception as e:
            return {"status": "failed", "error": str(e)}
        with TemporaryDirectory(prefix="dml-script-worker-") as tmpd:
            with chdir(tmpd):
                try:
                    with dag:
                        runit(dag)
                    return {"status": "succeeded", "error": None}
                except Exception as e:
                    return {"status": "failed", "error": str(e)}
                finally:
                    dag.cache()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="daggerml script worker")
    parser.add_argument("-o", "--output", default="result.json", help="JSON result path or '-' for stdout")
    parser.add_argument("argv_ptr")
    args = parser.parse_args(argv or sys.argv[1:])
    result = run_payload(args.argv_ptr)
    encoded = json.dumps(result, separators=(",", ":"), sort_keys=True)
    if args.output == "-":
        sys.stdout.write(encoded)
        if not encoded.endswith("\n"):
            sys.stdout.write("\n")
    else:
        Path(args.output).write_text(encoded)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
