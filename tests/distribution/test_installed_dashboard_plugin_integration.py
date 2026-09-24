"""Installed provider metadata, compatible DAG and real HTTP render/cache/refresh."""

import json
import os
import socket
import subprocess
from pathlib import Path
from time import monotonic, sleep
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

import pytest

import daggerml.api as api

pytestmark = [pytest.mark.slow, pytest.mark.serial]


def _request(url, *, data=None):
    request = Request(url, data=None if data is None else json.dumps(data).encode(),
                      headers={} if data is None else {"Content-Type": "application/json"})
    try:
        with urlopen(request, timeout=5) as response:
            return json.load(response)
    except HTTPError as exc:
        raise AssertionError(f"{url}: {exc.code} {exc.read().decode()}") from exc


def test_installed_provider_renders_caches_and_refreshes(installed_wheel, local_project, tmp_path):
    home = tmp_path / "plugin-project"
    dml = local_project("plugin-project")
    with api.new("metrics", dml=dml, tags=["example.metrics.v1"]) as dag:
        dag.commit(dag.put(42, name="answer"))
    ref = api.load("metrics", dml=dml).ref
    plugin = Path(__file__).resolve().parents[1] / "dashboard" / "fixtures" / "dashboard-plugin"
    python = installed_wheel / "bin" / "python"
    subprocess.run([str(python), "-m", "pip", "install", "--no-deps", str(plugin)],
                   check=True, capture_output=True, timeout=120)
    subprocess.run([str(python), "-I", "-c", "from importlib.metadata import entry_points; "
                    "assert any(e.name == 'example' for e in entry_points(group='daggerml.dashboards'))"],
                   check=True, cwd=tmp_path, timeout=15)
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        port = sock.getsockname()[1]
    env = {**os.environ, "PYTHONPATH": "", "DML_CONFIG_HOME": str(tmp_path / "config")}
    env.pop("DML_REMOTE_ROOT", None)
    process = subprocess.Popen(
        [str(installed_wheel / "bin" / "dml-dashboard"), "--port", str(port), "--no-open",
         "--config-home", str(tmp_path / "config")], cwd=tmp_path, env=env,
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
    )
    base = f"http://127.0.0.1:{port}"
    try:
        deadline = monotonic() + 15
        while True:
            try:
                _request(base + "/api/v1/health")
                break
            except URLError:
                if monotonic() > deadline or process.poll() is not None:
                    raise
                sleep(0.1)
        project = _request(base + "/api/v1/projects", data={"path": str(home)})["id"]
        scope = urlencode({"project": project, "revision": dml.status()["commit"].id()})
        prefix = f"{base}/api/v1/dags/{ref.to}/dashboard"
        metadata = _request(f"{prefix}s?{scope}")
        assert metadata["default"] == "example.nodes.plotly"
        selected = _request(f"{prefix}?name=example.nodes.plotly&{scope}")
        cached = _request(f"{prefix}?name=example.nodes.plotly&{scope}")
        refreshed = _request(f"{prefix}/refresh?{scope}", data={"name": "example.nodes.plotly"})
        assert selected["kind"] == "plotly"
        assert selected["cache_hit"] is False
        assert cached["cache_hit"] is True
        assert refreshed["cache_hit"] is False
        assert selected["data"][0]["x"] == ["answer"]
    finally:
        process.terminate()
        try:
            process.wait(timeout=10)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=5)
            raise
