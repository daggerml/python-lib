"""Installed wheel command lifecycle against an independent Moto endpoint."""

import json
import os
import subprocess

import pytest

pytestmark = [pytest.mark.slow, pytest.mark.serial]


def test_wheel_cli_authors_and_shares_shallow_project(installed_wheel, remote_env, s3_bucket, tmp_path):
    del remote_env, s3_bucket
    scripts = installed_wheel / "bin"
    dml = str(scripts / "dml")
    python = str(scripts / "python")
    env = {**os.environ, "PYTHONPATH": "", "DML_CONFIG_HOME": str(tmp_path / "config")}
    env.pop("DML_REMOTE_ROOT", None)
    root = f"s3://test-bucket/test-prefix/installed-{tmp_path.name}"
    source = tmp_path / "source"
    source.mkdir()

    def run(*args, cwd=source, input=None):
        completed = subprocess.run([dml, *args], cwd=cwd, env=env, input=input, text=True,
                                   capture_output=True, timeout=30)
        assert completed.returncode == 0, f"{args}: {completed.stderr}"
        return completed.stdout.strip()

    discovery = subprocess.run(
        [python, "-I", "-c", "from importlib.metadata import entry_points; "
         "assert {'dml','dml-dashboard','dml-local-adapter'} <= "
         "{ep.name for ep in entry_points(group='console_scripts')}"],
        cwd=tmp_path, env=env, capture_output=True, text=True, timeout=30,
    )
    assert discovery.returncode == 0, discovery.stderr
    run("init")
    run("config", "set", "remote.root", root)
    assert json.loads(run("status"))["branch"] == "main"
    index = run("runtime", "create")
    node = run("runtime", "put-literal", index, "-", "--name", "value", input='["scalar",42]\n')
    run("runtime", "commit", index, node, "--name", "installed-result")
    assert json.loads(run("log"))
    assert any(item["name"] == "main" for item in json.loads(run("branch", "list")))
    run("tag", "create", "installed-v1")
    assert any(item["name"] == "installed-v1" for item in json.loads(run("tag", "list")))
    run("push")

    clone = tmp_path / "clone"
    clone.mkdir()
    run("--remote-root", root, "clone", "main", "--depth", "1", "--project-home", ".", cwd=clone)
    show = json.loads(run("show", cwd=clone))
    assert "installed-result" in show["dags"]
    value_ref = run("dag", "get-node-by-name", show["dags"]["installed-result"], "value", cwd=clone)
    assert json.loads(run("dag", "get-node", value_ref, cwd=clone))[1] == 42
    run("fetch", "--unshallow", "main", cwd=clone)
