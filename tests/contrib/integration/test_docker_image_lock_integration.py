"""The image lifecycle lock survives fresh adapter process boundaries."""

import os
import select
import subprocess
import sys
from pathlib import Path

import pytest

from daggerml.contrib.executors import docker as docker_module

pytestmark = [pytest.mark.slow, pytest.mark.serial]


@pytest.mark.parametrize("cache_home", [None, "", "custom-cache"])
def test_image_lock_respects_xdg_cache_home(tmp_path, cache_home):
    env = dict(os.environ)
    if cache_home is None:
        env.pop("XDG_CACHE_HOME", None)
    else:
        env["XDG_CACHE_HOME"] = str(tmp_path / cache_home) if cache_home else ""
    script = "from daggerml.contrib.executors.docker import _IMAGE_LOCK_PATH; print(_IMAGE_LOCK_PATH)"
    result = subprocess.run(
        [sys.executable, "-c", script],
        env=env, capture_output=True, text=True, check=True, timeout=10,
    )
    cache = tmp_path / cache_home if cache_home else Path.home() / ".cache"
    assert Path(result.stdout.strip()) == cache / "daggerml" / "docker-images.lock"


def test_image_lock_coordinates_fresh_processes(tmp_path, monkeypatch):
    path = tmp_path / "images.lock"
    monkeypatch.setattr(docker_module, "_IMAGE_LOCK_PATH", path)
    child = None
    script = """
import sys
from pathlib import Path
from daggerml.contrib.executors import docker
docker._IMAGE_LOCK_PATH = Path(sys.argv[1])
print('ready', flush=True)
with docker._image_lock():
    print('acquired', flush=True)
"""
    try:
        with docker_module._image_lock():
            child = subprocess.Popen([sys.executable, "-c", script, str(path)],
                                     stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
            assert select.select([child.stdout], [], [], 10)[0], "child did not become ready"
            assert child.stdout.readline().strip() == "ready"
            assert not select.select([child.stdout], [], [], 0.1)[0], "child bypassed the held lock"
        stdout, stderr = child.communicate(timeout=10)
        assert child.returncode == 0, stderr
        assert stdout.strip() == "acquired"
    finally:
        if child is not None and child.poll() is None:
            child.terminate()
            child.wait(timeout=5)
