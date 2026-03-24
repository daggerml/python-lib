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
from contextlib import chdir
from pathlib import Path
from tempfile import TemporaryDirectory
from textwrap import dedent
from typing import Any, cast

import daggerml as dml
from daggerml._internal.types import DmlRepoError, Runnable, Uri
from daggerml.contrib.executor_state import LocalState, is_stale
from daggerml.contrib.executors._base import ExecutorBase
from daggerml.contrib.s3 import S3Store

logger = logging.getLogger(__name__)


META_KEY = "__dml_script_exec__"


class ScriptExecutor(ExecutorBase):
    name = "script"
    adapter = "local"
    state_class = LocalState

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

    @classmethod
    def start(cls, runnable, argv_ptr, cache_key, remote, state=None):
        workdir = Path(tempfile.mkdtemp(prefix=f"dml-script-{cache_key[:8]}-"))
        payload_path = workdir / "supervisor-input.json"
        result_path = workdir / "result.json"
        stdout_path = workdir / "stdout.log"
        stderr_path = workdir / "stderr.log"
        payload = {
            "version": 1,
            "cache_key": cache_key,
            "cmd": ["python", "-m", "daggerml.contrib.executors.script", argv_ptr],
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
                env={**os.environ, "PYTHONUNBUFFERED": "1", **payload["env"]},
            )
        initial = state.init_record(
            status="running",
        )
        state.put_if_absent(initial)
        with_meta = state.set_executor_metadata(
            executor_id=cls.name,
            data={
                "pid": proc.pid,
                "workdir": str(workdir),
                "result_path": str(result_path),
                "stdout_path": str(stdout_path),
                "stderr_path": str(stderr_path),
            },
        )
        state.update(with_meta)
        return {"status": "running", "error": None}

    @classmethod
    def poll(cls, state):
        record = state.get()
        status = record.get("status")
        if status in {"succeeded", "failed", "canceled"}:
            return {"status": status, "error": record.get("error")}
        if is_stale(record):
            return {"status": "failed", "error": "Script supervisor heartbeat stale"}
        return {"status": "running", "error": None}

    @classmethod
    def gc(cls, state):
        script_meta = state.get_executor_metadata(cls.name)
        if "pid" in script_meta:
            try:
                os.killpg(script_meta["pid"], signal.SIGTERM)
            except (ProcessLookupError, PermissionError):
                pass
        if "workdir" in script_meta:
            shutil.rmtree(script_meta["workdir"], ignore_errors=True)
        state.delete()


def _terminal_runnable(root: Runnable) -> Runnable:
    current = root
    while current.sub is not None:
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
        script = S3Store().get(script_uri).decode("utf-8")
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
            dag = dml_instance.new(argv_ptr=argv_ptr)
        except Exception as e:
            return {"status": "failed", "error": str(e)}
        with TemporaryDirectory(prefix="dml-script-worker-") as tmpd, chdir(tmpd):
            try:
                with dag:
                    runit(dag)
                return {"status": "succeeded", "error": None}
            except Exception as e:
                if dag.ref is not None:
                    return {"status": "succeeded", "error": None}
                return {"status": "failed", "error": str(e)}


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
