import json
import shutil
import sys
from io import StringIO

import pytest

from daggerml._cli import cli
from daggerml.api import Dml, load, new

pytestmark = pytest.mark.slow


def _run_cli(args: list[str]) -> tuple[str, str]:
    old_argv = sys.argv
    old_stdout = sys.stdout
    old_stderr = sys.stderr
    sys.argv = ["dml", *args]
    out = StringIO()
    err = StringIO()
    sys.stdout = out
    sys.stderr = err
    try:
        cli()
        return out.getvalue(), err.getvalue()
    finally:
        sys.argv = old_argv
        sys.stdout = old_stdout
        sys.stderr = old_stderr


def test_cli_full_project_lifecycle_across_two_repos(tmp_path):
    owner = "alice"
    name0 = "research0"
    name1 = "research1"
    remote_uri = "s3://test-bucket/test-prefix"
    source_repo = tmp_path / "source"
    source_repo.mkdir()

    stdout, stderr = _run_cli(
        [
            "--project-home",
            str(source_repo),
            "init",
            "--owner",
            owner,
            "--remote-uri",
            remote_uri,
            name0,
        ]
    )
    assert not stderr
    assert json.loads(stdout)["created"] == {"db": True, "config": True}

    expected_result = {"score": 7, "ok": True}
    dml = Dml(project_home=str(source_repo), user=owner)

    with new(dml=dml, name="baseline", message="baseline") as dag:
        result = dag.put(expected_result, name="result")
        dag.commit(result)
    with new(dml=dml, name="candidate", message="candidate") as dag:
        candidate = dag.put(11, name="candidate")
        dag.commit(candidate)

    stdout, stderr = _run_cli(["--project-home", str(source_repo), "push", "--branch", "main", "--create"])
    assert not stderr
    pushed_ref_path = json.loads(stdout)
    assert pushed_ref_path.endswith(f"/{name0}/heads/main.json")
    owner_from_remote = pushed_ref_path.split("/")[1]

    shutil.rmtree(source_repo)

    target_repo = tmp_path / "target"
    target_repo.mkdir()
    stdout, stderr = _run_cli(
        [
            "--project-home",
            str(target_repo),
            "init",
            "--owner",
            owner,
            "--remote-uri",
            remote_uri,
            name1,
        ]
    )
    assert not stderr
    assert json.loads(stdout)["created"] == {"db": True, "config": True}

    source_uri = f"dml://{owner_from_remote}/{name0}"
    stdout, stderr = _run_cli(["--project-home", str(target_repo), "fetch", source_uri])
    assert not stderr
    assert "commit:" in json.loads(stdout)

    stdout, stderr = _run_cli(
        [
            "--project-home",
            str(target_repo),
            "dag",
            "checkout",
            f"{source_uri}#main",
            "baseline",
            "--user",
            owner_from_remote,
        ]
    )
    assert not stderr
    assert "commit:" in json.loads(stdout)

    dml = Dml(project_home=str(target_repo), user=owner)
    assert load("baseline", dml=dml).result.value() == expected_result
