#!/usr/bin/env python3
import json
import shutil
import subprocess
import sys
from urllib.parse import urlparse


def _ensure_execution_record(payload: dict) -> None:
    cache_key = payload.get("cache_key")
    execution_id = payload.get("execution_id")
    remote_root = (payload.get("remote") or {}).get("root")
    if not isinstance(cache_key, str) or not isinstance(execution_id, str) or not isinstance(remote_root, str):
        return


def _run_adapter(adapter_name: str, payload: dict) -> int:
    adapter_path = shutil.which(adapter_name) if "/" not in adapter_name else adapter_name
    if adapter_path is None:
        sys.stderr.write(f"No such adapter: {adapter_name}\n")
        return 1
    cmd = [adapter_path]
    if adapter_path.endswith(".py"):
        cmd = [sys.executable, adapter_path]
    completed = subprocess.run(cmd, input=json.dumps(payload), text=True, capture_output=True, check=False)
    sys.stdout.write(completed.stdout)
    sys.stderr.write(completed.stderr)
    return completed.returncode


def _run_target(target: str, raw: str) -> int:
    script = urlparse(target).path
    completed = subprocess.run([sys.executable, script], input=raw, text=True, capture_output=True, check=False)
    sys.stdout.write(completed.stdout)
    sys.stderr.write(completed.stderr)
    return completed.returncode


def main() -> None:
    raw = sys.stdin.read()
    payload = json.loads(raw)
    _ensure_execution_record(payload)
    runnable = payload.get("runnable", {})
    sub = runnable.get("sub")
    if sub is not None:
        forwarded = {
            "argv_ptr": payload.get("argv_ptr"),
            "cache_key": payload.get("cache_key"),
            "execution_id": payload.get("execution_id"),
            "remote": payload.get("remote"),
            "state": None,
            "runnable": sub,
        }
        code = _run_adapter(sub.get("adapter", ""), forwarded)
        raise SystemExit(code)
    code = _run_target(runnable.get("target", ""), raw)
    raise SystemExit(code)


if __name__ == "__main__":
    main()
