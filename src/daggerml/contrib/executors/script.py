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
import traceback
from contextlib import chdir
from pathlib import Path
from textwrap import dedent
from typing import Any

import daggerml as dml
from daggerml import Runnable, Uri
from daggerml._core import AdapterCancelResponse, AdapterInvokeResponse
from daggerml.api import DmlRepoError
from daggerml.contrib.executors._base import ExecutorBase
from daggerml.contrib.s3 import S3Store

logger = logging.getLogger(__name__)


class ScriptExecutor(ExecutorBase):
    name = "script"
    adapter = "local"

    ############################# resolve runnable #############################
    ############################################################################

    @classmethod
    def resolve_runnable(cls, uri, kwargs, sub):
        if sub is not None:
            raise DmlRepoError("script executor does not accept sub runnable")
        resolved_kwargs, script = cls._script_kwargs(dict(kwargs))
        script_uri = S3Store().put(data=script.encode("utf-8"), suffix=".py")
        resolved_kwargs["script_uri"] = script_uri.uri
        return Runnable(target=Uri("script"), kwargs=resolved_kwargs, sub=sub, adapter="dml-local-adapter")

    @classmethod
    def _script_kwargs(cls, kwargs: dict) -> tuple[dict, str]:
        allowed = {"fn", "prepop", "extra_objs", "post_lines"}
        unknown = sorted(set(kwargs.keys()) - allowed)
        if unknown:
            bad = ", ".join(unknown)
            raise DmlRepoError(f"Unknown script executor kwargs: {bad}")
        fn = kwargs["fn"]
        prepop = kwargs.get("prepop", {})
        extra_objs = list(kwargs.get("extra_objs", []))
        post_lines = list(kwargs.get("post_lines", []))
        params = list(inspect.signature(fn).parameters.values())
        if not params or params[0].name != "dag":
            raise DmlRepoError("script fn must include first 'dag' parameter")
        script = cls._render_script(fn, extra_objs=extra_objs, post_lines=post_lines)
        return {"prepop": prepop, "fn_name": fn.__name__}, script

    @classmethod
    def _render_script(cls, fn, extra_objs: list, post_lines: list[str]) -> str:
        chunks: list[str] = []
        for obj in [*extra_objs, fn]:
            try:
                raw = dedent(inspect.getsource(inspect.unwrap(obj))).strip()
                chunks.append(cls._strip_funkify_decorators(raw))
            except (OSError, TypeError) as e:
                raise DmlRepoError(f"Failed to serialize object source: {e}") from e
        if post_lines:
            chunks.extend(post_lines)
        script = "\n".join(["\n\n".join(chunks), "\n"])
        try:
            mod = ast.parse(script)
        except SyntaxError as e:
            raise DmlRepoError(f"Generated script is not valid Python: {e}") from e
        if not any(isinstance(n, ast.FunctionDef) and n.name == fn.__name__ for n in mod.body):
            raise DmlRepoError(f"Function '{fn.__name__}' is not globally defined in generated script")
        return script

    @staticmethod
    def _strip_funkify_decorators(source: str) -> str:
        module = ast.parse(source)
        for node in module.body:
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                node.decorator_list = []
        return ast.unparse(module).strip()

    ############################# start/poll/cancel ############################

    def start(
        self,
        cache_key: str,
        execution_id: str,
        runnable: dict[str, Any],
        remote: dict[str, str],
        scratch_uri: str,
    ) -> AdapterInvokeResponse:
        del runnable, scratch_uri
        workdir = Path(tempfile.mkdtemp(prefix=f"dml-script-{execution_id[:8]}-"))
        payload_path = workdir / "supervisor-input.json"
        result_path = workdir / "result.json"
        stdout_path = workdir / "stdout.log"
        stderr_path = workdir / "stderr.log"
        payload = {
            "version": 0,
            "cache_key": cache_key,
            "execution_id": execution_id,
            "cmd": [
                sys.executable,
                "-m",
                "daggerml.contrib.executors.script",
                "--execution-id",
                execution_id,
                "--cache-key",
                cache_key,
                "--remote-root",
                remote["root"],
            ],
            "remote": remote,
            "env": {},
        }
        payload_path.write_text(json.dumps(payload, separators=(",", ":"), sort_keys=True))
        with stdout_path.open("w") as stdout_f, stderr_path.open("w") as stderr_f:
            proc = subprocess.Popen(
                [
                    sys.executable,
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
        launch_state = {
            "pid": proc.pid,
            "workdir": str(workdir),
            "result_path": str(result_path),
            "stdout_path": str(stdout_path),
            "stderr_path": str(stderr_path),
        }
        return {"status": "running", "error": None, "state": launch_state, "dag_id": None}

    def poll(
        self,
        cache_key: str,
        execution_id: str,
        runnable: dict[str, Any],
        state: dict[str, Any],
        remote: dict[str, str],
        scratch_uri: str,
    ) -> AdapterInvokeResponse:
        del cache_key, execution_id, runnable, remote, scratch_uri
        result_path = Path(state.get("result_path", ""))
        pid = state.get("pid")
        # Polls may run either in the launching adapter process or in a later
        # process. Reap children when we can; otherwise fall back to a direct
        # PID probe for cross-process polling.
        if isinstance(pid, int):
            try:
                done_pid, _ = os.waitpid(pid, os.WNOHANG)
                if done_pid == 0:
                    return {"status": "running", "error": None, "state": state, "dag_id": None}
            except ChildProcessError:
                try:
                    os.kill(pid, 0)
                    return {"status": "running", "error": None, "state": state, "dag_id": None}
                except ProcessLookupError:
                    pass
                except PermissionError:
                    return {"status": "running", "error": None, "state": state, "dag_id": None}
        # Process exited — read result
        if result_path.exists():
            try:
                parsed = json.loads(result_path.read_text())
                if parsed.get("status") in {"succeeded", "failed"}:
                    _cleanup_workdir(state)
                    return parsed
            except Exception as e:
                _cleanup_workdir(state)
                return {
                    "status": "failed",
                    "error": f"Could not read supervisor result: {e}",
                    "state": None,
                    "dag_id": None,
                }
        _cleanup_workdir(state)
        return {
            "status": "failed",
            "error": "Script supervisor exited without result",
            "state": None,
            "dag_id": None,
        }

    def cancel(
        self,
        cache_key: str,
        execution_id: str,
        runnable: dict[str, Any],
        state: dict[str, Any],
        remote: dict[str, str],
        scratch_uri: str,
        cancel_requested_by: str | None,
    ) -> AdapterCancelResponse:
        del cache_key, execution_id, runnable, remote, scratch_uri, cancel_requested_by
        if not isinstance(state, dict):
            return {"status": "cancelled", "error": None}
        pid = state.get("pid")
        if isinstance(pid, int):
            try:
                os.killpg(pid, signal.SIGTERM)
            except ProcessLookupError:
                pass
            except PermissionError:
                pass
        _cleanup_workdir(state)
        return {"status": "cancelled", "error": None}


def _cleanup_workdir(launch_state: dict[str, Any]) -> None:
    workdir = launch_state.get("workdir")
    if isinstance(workdir, str) and workdir:
        shutil.rmtree(workdir, ignore_errors=True)


def run_payload(*, execution_id: str, cache_key: str, remote_root: str) -> dict[str, Any]:
    namespace: dict[str, Any] = {"logger": logging.getLogger("daggerml.contrib.script")}
    with dml.temporary(remote_root=remote_root) as tmpdml, chdir(tmpdml._config.project_home):
        try:
            with dml.new(dml=tmpdml, cache_key=cache_key, execution_id=execution_id) as dag:
                runnable_node, *arg_nodes = dag.argv
                runnable = runnable_node.value().innermost()
                for key, value in runnable.kwargs.get("prepop", {}).items():
                    dag.put(value, name=key)
                exec(S3Store().get(runnable.kwargs["script_uri"]).decode("utf-8"), namespace)
                fn = namespace.get(runnable.kwargs["fn_name"])
                output = fn(dag, *arg_nodes)
                if dag.ref is None:
                    dag.commit(output)
            if dag.ref is None:
                raise DmlRepoError("Script worker succeeded without committed DAG")
            return {"status": "succeeded", "state": None, "error": None, "dag_id": dag.ref.id()}
        except Exception as e:
            return {"status": "failed", "error": f"{e}\n{traceback.format_exc()}", "state": None, "dag_id": None}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="daggerml script worker")
    parser.add_argument("-o", "--output", default="result.json", help="JSON result path or '-' for stdout")
    parser.add_argument("--execution-id", required=True)
    parser.add_argument("--cache-key", required=True)
    parser.add_argument("--remote-root", required=True)
    args = parser.parse_args(argv or sys.argv[1:])
    result = run_payload(execution_id=args.execution_id, cache_key=args.cache_key, remote_root=args.remote_root)
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
