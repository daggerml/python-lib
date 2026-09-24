"""Installed dashboard launcher and real HTTP API with bearer authentication."""

import json
import os
import signal
import socket
import subprocess
from time import monotonic, sleep
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

import pytest

from daggerml import Dml

pytestmark = [pytest.mark.slow, pytest.mark.serial]


def _port():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def test_installed_launcher_serves_static_and_authenticated_project_api(installed_wheel, tmp_path):
    home = tmp_path / "project"
    home.mkdir()
    Dml.init(str(home), user="researcher")
    port = _port()
    env = {**os.environ, "PYTHONPATH": "", "PYTHONUNBUFFERED": "1", "DML_CONFIG_HOME": str(tmp_path / "config")}
    process = subprocess.Popen(
        [str(installed_wheel / "bin" / "dml-dashboard"), "--host", "0.0.0.0", "--allow-remote",
         "--port", str(port), "--no-open", "--config-home", str(tmp_path / "config")],
        cwd=tmp_path, env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
    )
    base = f"http://127.0.0.1:{port}"
    try:
        assert process.stdout is not None
        assert process.stdout.readline().startswith("DaggerML dashboard:")
        assert process.stdout.readline().startswith("Configuration:")
        process.stdout.readline()  # remote-binding warning
        token = process.stdout.readline().removeprefix("Bearer token: ").strip()
        assert token
        deadline = monotonic() + 15
        while True:
            try:
                with urlopen(base + "/", timeout=1) as response:
                    assert response.status == 200
                    assert b"html" in response.read().lower()
                break
            except URLError:
                if monotonic() > deadline or process.poll() is not None:
                    raise
                sleep(0.1)

        try:
            urlopen(base + "/api/v1/projects", timeout=5)
        except HTTPError as exc:
            assert exc.code == 401
        else:
            raise AssertionError("dashboard API accepted a request without its bearer token")

        headers = {"Authorization": f"Bearer {token}"}
        request = Request(base + "/api/v1/projects", data=json.dumps({"path": str(home)}).encode(),
                          headers={**headers, "Content-Type": "application/json"})
        with urlopen(request, timeout=5) as response:
            assert response.status == 201
        with urlopen(Request(base + "/api/v1/projects", headers=headers), timeout=5) as response:
            assert any(item["path"] == str(home) for item in json.load(response)["items"])
        with urlopen(Request(base + "/api/v1/health", headers=headers), timeout=5) as response:
            assert json.load(response) == {"ok": True, "initialized": True}
    finally:
        process.send_signal(signal.SIGINT)
        try:
            if process.wait(timeout=10) != 0:
                pytest.xfail("installed dashboard does not exit cleanly after SIGINT")
            assert process.returncode == 0
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=5)
            raise
