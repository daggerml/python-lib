"""Manage a local moto server and a sourceable env file for examples.

This helper owns the infra lifecycle for moto-backed example scripts. It keeps
state under a caller-provided directory so repeated `up` calls with the same
directory are idempotent.
"""

from __future__ import annotations

import argparse
import json
import os
import platform
import shlex
import shutil
import signal
import socket
import subprocess
import sys
import time
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.parse import urlparse
from urllib.request import urlopen

import boto3

ENV_FILE_NAME = "moto.env"
LOG_FILE_NAME = "moto.log"
PID_FILE_NAME = "moto.pid"
STATE_FILE_NAME = "moto.json"


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)

    for name in ("up", "down", "print-env", "serve"):
        child = subparsers.add_parser(name)
        child.add_argument("--moto-dir", required=True, type=Path)
        if name in {"up", "serve"}:
            child.add_argument("--remote-root", default="s3://daggerml-example/artifacts")

    return parser.parse_args()


def _pid_path(moto_dir: Path) -> Path:
    return moto_dir / PID_FILE_NAME


def _env_path(moto_dir: Path) -> Path:
    return moto_dir / ENV_FILE_NAME


def _log_path(moto_dir: Path) -> Path:
    return moto_dir / LOG_FILE_NAME


def _state_path(moto_dir: Path) -> Path:
    return moto_dir / STATE_FILE_NAME


def _parse_remote_root(remote_root: str) -> tuple[str, str]:
    parsed = urlparse(remote_root)
    if parsed.scheme != "s3" or not parsed.netloc:
        raise RuntimeError(f"remote root must be s3://bucket[/prefix], got: {remote_root!r}")
    return parsed.netloc, parsed.path.lstrip("/")


def _build_env_values(endpoint: str, remote_root: str) -> dict[str, str]:
    return {
        "AWS_ACCESS_KEY_ID": "test",
        "AWS_SECRET_ACCESS_KEY": "test",
        "AWS_REGION": "us-east-1",
        "AWS_DEFAULT_REGION": "us-east-1",
        "AWS_SHARED_CREDENTIALS_FILE": "/dev/null",
        "AWS_ENDPOINT_URL": endpoint,
        "DML_REMOTE_ROOT": remote_root,
    }


def _write_env_file(path: Path, env_values: dict[str, str]) -> None:
    text = "".join(f"export {key}={shlex.quote(value)}\n" for key, value in env_values.items())
    path.write_text(text, encoding="utf-8")


def _read_env_file(path: Path) -> dict[str, str]:
    env: dict[str, str] = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or not line.startswith("export "):
            continue
        key, value = line[len("export ") :].split("=", 1)
        env[key] = shlex.split(value)[0]
    return env


def _read_pid(path: Path) -> int | None:
    try:
        return int(path.read_text(encoding="utf-8").strip())
    except (OSError, ValueError):
        return None


def _process_alive(pid: int | None) -> bool:
    if pid is None or pid <= 0:
        return False
    try:
        os.kill(pid, 0)
    except OSError:
        return False
    return True


def _state_matches(moto_dir: Path, remote_root: str) -> bool:
    env_path = _env_path(moto_dir)
    pid = _read_pid(_pid_path(moto_dir))
    if not env_path.exists() or not _process_alive(pid):
        return False
    try:
        env = _read_env_file(env_path)
    except OSError:
        return False
    return env.get("DML_REMOTE_ROOT") == remote_root and "AWS_ENDPOINT_URL" in env


def _wait_for_ready(moto_dir: Path, pid: int, timeout: float = 60.0) -> None:
    env_path = _env_path(moto_dir)
    deadline = time.time() + timeout
    while time.time() < deadline:
        if env_path.exists() and env_path.stat().st_size > 0:
            return
        if not _process_alive(pid):
            break
        time.sleep(0.1)
    log_text = _log_path(moto_dir).read_text(encoding="utf-8") if _log_path(moto_dir).exists() else ""
    raise RuntimeError(f"timed out waiting for moto server readiness\n{log_text}".rstrip())


def _reserve_local_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def _wait_for_http_ready(endpoint: str, proc: subprocess.Popen[str], timeout: float = 30.0) -> None:
    deadline = time.time() + timeout
    while time.time() < deadline:
        if proc.poll() is not None:
            raise RuntimeError(f"moto server exited before becoming ready with code {proc.returncode}")
        try:
            with urlopen(endpoint, timeout=1.0):
                return
        except HTTPError:
            # Any HTTP response means the server is accepting connections.
            return
        except URLError:
            time.sleep(0.1)
    raise RuntimeError(f"timed out waiting for moto server HTTP readiness at {endpoint}")


def _server_binding(port: int) -> tuple[str, str]:
    endpoint = f"http://127.0.0.1:{port}"
    if platform.system() == "Darwin":
        return "127.0.0.1", endpoint
    return "0.0.0.0", endpoint


def _cleanup_files(moto_dir: Path) -> None:
    for path in (_env_path(moto_dir), _pid_path(moto_dir), _state_path(moto_dir)):
        try:
            path.unlink()
        except FileNotFoundError:
            pass


def cmd_up(moto_dir: Path, remote_root: str) -> int:
    moto_dir.mkdir(parents=True, exist_ok=True)
    if _state_matches(moto_dir, remote_root):
        print(_env_path(moto_dir))
        return 0

    cmd_down(moto_dir)
    moto_dir.mkdir(parents=True, exist_ok=True)
    log_path = _log_path(moto_dir)
    with log_path.open("w", encoding="utf-8") as log_file:
        proc = subprocess.Popen(
            [
                sys.executable,
                __file__,
                "serve",
                "--moto-dir",
                str(moto_dir),
                "--remote-root",
                remote_root,
            ],
            stdout=log_file,
            stderr=subprocess.STDOUT,
            start_new_session=True,
        )
    _wait_for_ready(moto_dir, proc.pid)
    print(_env_path(moto_dir))
    return 0


def cmd_down(moto_dir: Path) -> int:
    pid = _read_pid(_pid_path(moto_dir))
    if _process_alive(pid):
        assert pid is not None
        os.kill(pid, signal.SIGTERM)
        deadline = time.time() + 10.0
        while time.time() < deadline and _process_alive(pid):
            time.sleep(0.1)
        if _process_alive(pid):
            os.kill(pid, signal.SIGKILL)
    shutil.rmtree(moto_dir, ignore_errors=True)
    return 0


def cmd_print_env(moto_dir: Path) -> int:
    sys.stdout.write(_env_path(moto_dir).read_text(encoding="utf-8"))
    return 0


def cmd_serve(moto_dir: Path, remote_root: str) -> int:
    moto_server = shutil.which("moto_server")
    if moto_server is None:
        raise RuntimeError("Install moto[server] to run this helper")

    bucket, _prefix = _parse_remote_root(remote_root)
    moto_dir.mkdir(parents=True, exist_ok=True)
    _pid_path(moto_dir).write_text(str(os.getpid()), encoding="utf-8")

    stop = False

    def _handle_signal(_signum: int, _frame: object) -> None:
        nonlocal stop
        stop = True

    signal.signal(signal.SIGTERM, _handle_signal)
    signal.signal(signal.SIGINT, _handle_signal)

    port = _reserve_local_port()
    bind_host, endpoint = _server_binding(port)
    print("Starting moto server", file=sys.stderr, flush=True)
    proc = subprocess.Popen([moto_server, "-H", bind_host, "-p", str(port)])
    try:
        _wait_for_http_ready(endpoint, proc)
        print(f"Moto server listening at {endpoint}", file=sys.stderr, flush=True)
        env_values = _build_env_values(endpoint, remote_root)
        for key, value in env_values.items():
            os.environ[key] = value
        boto3.setup_default_session()
        print(f"Creating moto bucket {bucket}", file=sys.stderr, flush=True)
        boto3.client("s3", endpoint_url=endpoint).create_bucket(Bucket=bucket)
        _write_env_file(_env_path(moto_dir), env_values)
        _state_path(moto_dir).write_text(
            json.dumps({"pid": os.getpid(), "endpoint": endpoint, "remote_root": remote_root}, indent=2),
            encoding="utf-8",
        )
        while not stop:
            if proc.poll() is not None:
                raise RuntimeError(f"moto server exited unexpectedly with code {proc.returncode}")
            time.sleep(0.2)
    finally:
        if proc.poll() is None:
            proc.terminate()
            try:
                proc.wait(timeout=10.0)
            except subprocess.TimeoutExpired:
                proc.kill()
                proc.wait(timeout=10.0)
        _cleanup_files(moto_dir)
    return 0


def main() -> int:
    args = _parse_args()
    if args.command == "up":
        return cmd_up(args.moto_dir, args.remote_root)
    if args.command == "down":
        return cmd_down(args.moto_dir)
    if args.command == "print-env":
        return cmd_print_env(args.moto_dir)
    if args.command == "serve":
        return cmd_serve(args.moto_dir, args.remote_root)
    raise RuntimeError(f"unknown command: {args.command}")


if __name__ == "__main__":
    raise SystemExit(main())
