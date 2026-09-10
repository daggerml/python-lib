"""Run with an installed wheel's Python, not pytest or an editable checkout.

Usage: /path/to/venv/bin/python -I tests/distribution/check_installed_docs.py
Requires the dashboard extra and httpx in that environment. No build is run.
"""

from __future__ import annotations

import argparse
import io
import json
import os
import shutil
import sys
import tempfile
import zipfile
from importlib.metadata import distribution
from pathlib import Path, PurePosixPath
from unittest.mock import patch


def main():
    argparse.ArgumentParser(description=__doc__).parse_args()
    package = distribution("daggerml")
    assert sorted(package.requires or []) == sorted(
        [
            "boto3",
            'rich; extra == "terminal"',
            'fastapi<1,>=0.115; extra == "dashboard"',
            'uvicorn<1,>=0.32; extra == "dashboard"',
        ]
    ), package.requires
    assert sorted(package.metadata.get_all("Provides-Extra") or []) == ["dashboard", "terminal"]
    direct_url = json.loads(package.read_text("direct_url.json") or "{}")
    assert not direct_url.get("dir_info", {}).get("editable"), "An installed wheel is required"
    root = Path(package.locate_file("daggerml")).resolve()
    assert root.is_relative_to(Path(sys.prefix).resolve()), root
    docs = root / "dashboard/static/docs"
    manifest = json.loads((docs / "manifest.json").read_bytes())
    paths = ["manifest.json", *(page["fragment"] for page in manifest["pages"])]
    paths += manifest["assets"] + manifest["downloads"]
    assert any(path.startswith("fragments/") and path.count("/") >= 2 for path in paths)
    assert any(path.startswith("assets/") and path.count("/") >= 3 for path in paths)
    assert any(path.endswith(".py") and path.count("/") >= 4 for path in manifest["downloads"])
    assert any(path.endswith(".zip") for path in manifest["downloads"])
    for path in paths:
        relative = PurePosixPath(path)
        assert not relative.is_absolute() and ".." not in relative.parts, path
        assert (docs / path).is_file(), path

    # TestClient uses in-process ASGI transport: no fixture or HTTP socket is needed.
    # Audit hooks also catch absolute executable paths and swallowed service attempts.
    forbidden = []

    def reject_external_work(event, args):
        if event in {"socket.connect", "socket.getaddrinfo", "subprocess.Popen", "os.system", "os.posix_spawn"}:
            forbidden.append(event)
            raise AssertionError(f"Docs attempted external work: {event}")

    with tempfile.TemporaryDirectory(prefix="daggerml-installed-docs-") as temporary:
        environment = {
            "HOME": temporary,
            "PATH": temporary,
            "DML_CONFIG_HOME": str(Path(temporary) / "config"),
            "AWS_EC2_METADATA_DISABLED": "true",
            "AWS_SHARED_CREDENTIALS_FILE": str(Path(temporary) / "no-credentials"),
        }
        with patch.dict(os.environ, environment, clear=True):
            for tool in ("quarto", "R", "Rscript", "node", "npm", "moto_server", "ssh"):
                assert shutil.which(tool) is None, tool
            sys.addaudithook(reject_external_work)
            from fastapi.testclient import TestClient

            import daggerml
            from daggerml.dashboard.server import create_app

            assert Path(daggerml.__file__).resolve().parent == root, daggerml.__file__
            with TestClient(create_app(config_home=environment["DML_CONFIG_HOME"])) as client:
                headers = {"host": "127.0.0.1:8765"}
                shell = (root / "dashboard/static/index.html").read_bytes()
                for route in ("/docs", "/docs/examples/analysis-report"):
                    response = client.get(route, headers=headers)
                    assert response.status_code == 200, route
                    assert response.content == shell, route
                for path in paths:
                    response = client.get(f"/docs/static/{path}", headers=headers)
                    assert response.status_code == 200, path
                    assert response.content == (docs / path).read_bytes(), path
                    if path.endswith(".zip"):
                        with zipfile.ZipFile(io.BytesIO(response.content)) as bundle:
                            assert bundle.namelist(), path
                            for member in bundle.namelist():
                                assert f"downloads/examples/{member}" in manifest["downloads"], member
                                assert bundle.read(member) == (docs / "downloads/examples" / member).read_bytes()
                for path in ("fragments/missing.html", "assets/missing.png", "downloads/missing.py"):
                    assert client.get(f"/docs/static/{path}", headers=headers).status_code == 404, path
            assert not forbidden, forbidden
    print(f"PASS: {package.metadata['Name']} {package.version}: {len(paths)} packaged Docs files; "
          "unchanged dependency metadata; no build tools, processes, or service connections")


if __name__ == "__main__":
    main()
