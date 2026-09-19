from __future__ import annotations

import importlib.util
import json
import os
import shutil
import socket
import subprocess
import sys
import tarfile
import tomllib
import zipfile
from email import message_from_bytes
from pathlib import Path
from urllib.parse import urlparse

import pytest
import yaml

ROOT = Path(__file__).parents[1]
SPEC = importlib.util.spec_from_file_location("docs_build", ROOT / "docs/build.py")
assert SPEC and SPEC.loader
build = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(build)


def test_docs_build_015__source_aware_lessons_execute_inline(tmp_path):
    work = tmp_path / "source"
    build.validate()
    build.prepare(work)

    page = (work / "start-here/funks.qmd").read_text(encoding="utf-8")
    assert "engine: jupyter" in page
    assert "jupyter: python3" in page
    assert "@api.funkify(extra_objs=(clamp,)" in page
    assert "def summarize(dag, values):" in page
    assert "{{< dml-snippet" not in page
    assert "{{< dml-run" not in page
    assert "runpy.run_path" not in page
    assert not (ROOT / "docs/examples/start-here/funks.py").exists()

    dagclasses = (work / "start-here/dagclasses.qmd").read_text(encoding="utf-8")
    assert "engine: jupyter" in dagclasses
    assert "jupyter: python3" in dagclasses
    assert "class ScaledTotal:" in dagclasses
    assert "class ContainerTotal:" in dagclasses
    assert "{{< dml-snippet" not in dagclasses
    assert "{{< dml-run" not in dagclasses
    assert "runpy.run_path" not in dagclasses
    assert not (ROOT / "docs/examples/start-here/dagclasses.py").exists()


def test_docs_build_009__single_project_pages_export_frontmatter_project_home(tmp_path):
    work = tmp_path / "source"
    build.validate()
    build.prepare(work)

    expected = {
        "start-here/dags.qmd": "research-demo",
        "start-here/funks.qmd": "research-demo",
        "start-here/dagclasses.qmd": "research-demo",
    }
    dependencies = {
        "start-here/dags.qmd": ["start-here/get-started"],
        "start-here/funks.qmd": ["start-here/dags"],
        "start-here/dagclasses.qmd": ["start-here/funks"],
    }
    for relative, project_home in expected.items():
        authored = (ROOT / "docs" / relative).read_text(encoding="utf-8")
        prepared = (work / relative).read_text(encoding="utf-8")
        assert build.dml_project_home(ROOT / "docs" / relative, authored) == project_home
        if "engine: jupyter" in authored:
            assert f'dml_project_home = docs_workspace / "{project_home}"' in prepared
            assert 'os.environ["DML_PROJECT_HOME"] = str(dml_project_home)' in prepared
            assert "os.chdir(page_workdir)" in prepared
        else:
            assert f'dml_project_home <- file.path(docs_workspace, "{project_home}")' in prepared
            assert "Sys.setenv(DML_PROJECT_HOME = dml_project_home)" in prepared
        assert build.page_dependencies(ROOT / "docs" / relative, authored) == dependencies[relative]

    dags = (work / "start-here/dags.qmd").read_text(encoding="utf-8")
    assert 'Path(os.environ["DML_PROJECT_HOME"]).mkdir' not in dags
    assert 'Dml.init(os.environ["DML_PROJECT_HOME"]' not in dags
    assert "dml-source" not in dags
    assert "def " not in dags
    assert dags.count("```{python}") == 8
    assert 'dag = dml.new("docs-image"' in dags
    assert "```{python}\ndag.commit(image)\n```" in dags

    getting_started = (ROOT / "docs/start-here/get-started.qmd").read_text(encoding="utf-8")
    assert build.dml_project_home(ROOT / "docs/start-here/get-started.qmd", getting_started) is None
    assert build.page_dependencies(ROOT / "docs/start-here/get-started.qmd", getting_started) == ["start-here"]
    assert "```{bash}\nmkdir research-demo\ncd research-demo\ndml init\n```" in getting_started
    assert "```{python}" not in getting_started
    assert "project = dml.Dml" not in getting_started
    assert "dml=project" not in getting_started
    render = yaml.safe_load((work / "_quarto.yml").read_text(encoding="utf-8"))["project"]["render"]
    course = [
        "start-here/index.qmd",
        "start-here/get-started.qmd",
        "start-here/dags.qmd",
        "start-here/funks.qmd",
        "start-here/dagclasses.qmd",
    ]
    assert [render.index(page) for page in course] == sorted(render.index(page) for page in course)
    assert not (work / "index.qmd").exists()
    assert not (work / "getting-started.qmd").exists()
    assert not list((work / "examples").rglob("*.qmd"))
    assert "docs_workspace <- Sys.getenv(\"DOCS_WORKSPACE_ROOT\")" in dags
    assert "knitr::opts_knit$set(root.dir = page_workdir)" in dags


def test_docs_build_014__use_pages_allow_executable_cells(tmp_path):
    concepts = tmp_path / "use"
    concepts.mkdir()
    (concepts / "bad.qmd").write_text(
        "---\ntitle: Bad concept\nengine: knitr\n---\n\n```{python}\nprint('not pseudocode')\n```\n",
        encoding="utf-8",
    )
    build.validate(tmp_path)


@pytest.mark.parametrize("language", ["bash", "r"])
def test_docs_build_016__jupyter_pages_reject_non_python_executable_cells(tmp_path, language):
    (tmp_path / "bad.qmd").write_text(
        f"---\ntitle: Bad Jupyter page\nengine: jupyter\njupyter: python3\n---\n\n"
        f"```{{{language}}}\ntrue\n```\n",
        encoding="utf-8",
    )
    with pytest.raises(ValueError, match="jupyter pages may contain only Python"):
        build.validate(tmp_path)


@pytest.mark.parametrize("project_home", ["", "/absolute", "../outside", "nested/../../outside", "windows\\path"])
def test_docs_build_010__project_home_must_stay_inside_page_fixture(tmp_path, project_home):
    page = tmp_path / "bad.qmd"
    page.write_text(
        f"---\ntitle: Bad\nengine: knitr\ndml-project-home: '{project_home}'\n---\n",
        encoding="utf-8",
    )
    with pytest.raises(ValueError, match="dml-project-home"):
        build.validate(tmp_path)


def test_docs_build_011__dependency_graph_is_stable_and_topological(tmp_path):
    for name, dependencies in {
        "alpha": ["zulu"],
        "middle": [],
        "zulu": [],
    }.items():
        frontmatter = ["---", f"title: {name}", "engine: knitr"]
        if dependencies:
            frontmatter.extend(["depends-on:", *[f"  - {dependency}" for dependency in dependencies]])
        frontmatter.extend(["---", ""])
        (tmp_path / f"{name}.qmd").write_text("\n".join(frontmatter), encoding="utf-8")

    assert [page.stem for page in build.execution_order(tmp_path)] == ["zulu", "alpha", "middle"]


@pytest.mark.parametrize(
    ("frontmatter", "message"),
    [
        ("depends-on: missing", "unknown depends-on"),
        ("depends-on: page", "depend on itself"),
        ("depends-on: [base, base]", "duplicate page ID"),
        ("depends-on: ../base", "canonical page IDs"),
        ("depends-on: base.qmd", "canonical page IDs"),
    ],
)
def test_docs_build_012__dependency_graph_rejects_invalid_edges(tmp_path, frontmatter, message):
    (tmp_path / "base.qmd").write_text("---\ntitle: Base\nengine: knitr\n---\n", encoding="utf-8")
    (tmp_path / "page.qmd").write_text(
        f"---\ntitle: Page\nengine: knitr\n{frontmatter}\n---\n", encoding="utf-8"
    )
    with pytest.raises(ValueError, match=message):
        build.validate(tmp_path)


def test_docs_build_013__dependency_graph_rejects_cycles(tmp_path):
    (tmp_path / "alpha.qmd").write_text(
        "---\ntitle: Alpha\nengine: knitr\ndepends-on: beta\n---\n", encoding="utf-8"
    )
    (tmp_path / "beta.qmd").write_text(
        "---\ntitle: Beta\nengine: knitr\ndepends-on: alpha\n---\n", encoding="utf-8"
    )
    with pytest.raises(ValueError, match=r"dependency cycle: alpha -> beta -> alpha"):
        build.validate(tmp_path)


def test_docs_build_004__tooling_stays_out_of_published_dependencies():
    metadata = tomllib.loads((ROOT / "pyproject.toml").read_text(encoding="utf-8"))
    dependencies = metadata["project"]["dependencies"]
    optional = metadata["project"]["optional-dependencies"]
    build_tools = {"quarto", "knitr", "rmarkdown", "reticulate", "xfun", "jupyter", "ipykernel"}

    assert all(not any(tool in dependency.lower() for tool in build_tools) for dependency in dependencies)
    assert all(
        not any(tool in dependency.lower() for tool in build_tools)
        for group in optional.values()
        for dependency in group
    )


def test_docs_build__released_package_install_is_not_executed():
    page = (ROOT / "docs/start-here/get-started.qmd").read_text(encoding="utf-8")

    assert "```bash\npip install daggerml\n```" in page
    assert "```{bash}\npip install daggerml" not in page
    assert "PIP_TARGET" not in (ROOT / "docs/build.sh").read_text(encoding="utf-8")


@pytest.mark.parametrize(
    ("content", "message"),
    [
        ("---\nexecute:\n  eval: false\n---\n", "eval: false"),
        ("---\nexecute:\n  error: true\n---\n", "error: true"),
        ("---\nexecute:\n  cache: true\n---\n", "cache: true"),
        ("---\nexecute:\n  freeze: true\n---\n", "freeze: true"),
        ("```text\nnot checked\n```\n", "static 'text' fence"),
        ("```{python}\nimport subprocess\nsubprocess.run(['echo', 'no'])\n```\n", "Python shell wrappers"),
        ("```{r}\nknitr::opts_chunk$set(eval = FALSE)\n```\n", "eval: false"),
        ("---\nexecute: {freeze: auto}\n---\n", "freeze: auto"),
        ("---\nexecute: false\n---\n", "execute must"),
        ("~~~{python, eval=FALSE}\nassert False\n~~~\n", "eval: false"),
        ("````{python}\n#| eval: [1]\nassert False\n````\n", "eval:"),
        ("```{r}\nknitr::opts_chunk$set(error = 2)\n```\n", "error: 2"),
        ("```{python}\nfrom subprocess import run as launch\n```\n", "Python shell wrappers"),
        ("```{python, label}\nimport os as shell\nshell.system('false')\n```\n", "Python shell wrappers"),
        ("```\nuntyped authored code\n```\n", "static 'untyped' fence"),
        ("~~~python\nassert False\n~~~\n", "static 'python' fence"),
        ("<!-- docs:pseudocode: one only -->\n```text\nfirst\n```\n\n```python\nsecond\n```", "static 'python' fence"),
    ],
)
def test_docs_build_002__validation_rejects_execution_bypasses(tmp_path, content, message):
    page = tmp_path / "bad.qmd"
    page.write_text(content, encoding="utf-8")
    original = build.qmd_files
    build.qmd_files = lambda _root: [page]
    try:
        with pytest.raises(ValueError, match=message):
            build.validate(tmp_path)
    finally:
        build.qmd_files = original


@pytest.mark.parametrize("filename", ["_quarto.yml", "_quarto-ci.yml", "_metadata.yml"])
@pytest.mark.parametrize("setting", ["freeze: auto", "eval: false", "error: true", "cache: true"])
def test_docs_build_policy__project_and_directory_overrides_fail(tmp_path, filename, setting):
    (tmp_path / filename).write_text(f"execute:\n  {setting}\n", encoding="utf-8")
    with pytest.raises(ValueError, match="execution policy"):
        build.validate(tmp_path)


def test_docs_build_policy__only_authored_executable_options_are_validated(tmp_path):
    (tmp_path / "valid.qmd").write_text(
        "<!-- docs:pseudocode: deliberately disabled configuration -->\n"
        "```yaml\neval: false\n```\n"
        "~~~mermaid\ngraph LR; A-->B\n~~~\n"
        '```{python}\nprint("eval: false")\n```\n',
        encoding="utf-8",
    )
    build.validate(tmp_path)


def test_docs_build_003__staging_creates_script_free_fragments_and_manifest(tmp_path):
    render = tmp_path / "render/start-here/dags"
    render.mkdir(parents=True)
    (render / "index.html").write_text(
        '<html><body><main><h1 id="example">Example</h1>'
        '<a href="../../use/guide.md#next">Guide</a>'
        '<img src="chart.png"><script>secret()</script></main></body></html>',
        encoding="utf-8",
    )
    guide = tmp_path / "render/use"
    guide.mkdir()
    (guide / "guide.html").write_text('<main><h1 id="next">Guide</h1></main>', encoding="utf-8")
    (render / "chart.png").write_bytes(b"image")
    staging = tmp_path / "staging"
    build.stage(tmp_path / "render", staging)
    fragment = (staging / "fragments/start-here/dags.html").read_text(encoding="utf-8")
    manifest = json.loads((staging / "manifest.json").read_text(encoding="utf-8"))
    assert "<script" not in fragment
    assert 'href="/docs/use/guide#next"' in fragment
    assert 'src="/docs/static/assets/start-here/dags/chart.png"' in fragment
    assert manifest["pages"][0]["headings"] == [{"id": "example", "level": 1, "text": "Example"}]
    assert manifest["pages"][0]["title"] == "Example"


def test_docs_build_005__packaging_build_infers_components_and_supports_overrides():
    script = (ROOT / "docs/build.sh").read_text(encoding="utf-8")
    ci = (ROOT / ".github/workflows/ci.yml").read_text(encoding="utf-8")

    help_result = subprocess.run(
        ["bash", str(ROOT / "docs/build.sh"), "--help"], text=True, capture_output=True, check=True
    )
    assert "--auto" in help_result.stdout
    assert "--full" in help_result.stdout
    assert "--docs-only" in help_result.stdout
    assert "--ui-only" in help_result.stdout
    assert 'mode="auto"' in script
    assert 'docs_fingerprint="$(fingerprint docs src/daggerml pyproject.toml uv.lock)"' in script
    assert 'ui_fingerprint="$(fingerprint docs/build.sh dashboard-ui)"' in script
    assert 'build_state="$tools/dashboard-build-state"' in script
    assert 'if [[ "$docs_fingerprint" != "$stored_docs_fingerprint" ]] || ! docs_output_ready' in script
    assert 'if [[ "$ui_fingerprint" != "$stored_ui_fingerprint" ]] || ! ui_output_ready' in script
    assert 'rm -rf "$staging"' in script
    assert 'bash "$root/docs/build-render.sh"' in script
    assert "npm run build" in script
    assert 'rm -rf "$static/docs"' in script
    assert 'cp -R "$staging/." "$static/docs/"' in script
    assert script.index('rm -rf "$staging"') < script.index('bash "$root/docs/build-render.sh"')
    assert 'cp -R "$static/docs/." "$preserved_docs/docs/"' in script
    assert 'cp -R "$preserved_docs/docs/." "$static/docs/"' in script
    assert ci.count("run: bash ./docs/build.sh") == 3
    assert not (ROOT / "build-dashboard.sh").exists()


def test_docs_build__shared_entrypoint_bootstraps_an_isolated_pinned_toolchain():
    script = (ROOT / "docs/build.sh").read_text(encoding="utf-8")
    ci = (ROOT / ".github/workflows/ci.yml").read_text(encoding="utf-8")

    assert 'tools="${DML_DOCS_TOOLS_ROOT:-$root/.tools}"' in script
    assert 'MAMBA_ROOT_PREFIX="$docs_mamba"' in script
    assert 'CONDA_PKGS_DIRS="$docs_mamba/pkgs"' in script
    assert 'TMPDIR="$docs_tmp"' in script
    assert 'python="${DOCS_PYTHON:-$root/.venv/bin/python}"' in script
    assert 'QUARTO_PYTHON="$python"' in script
    for platform in ("osx-arm64", "osx-64", "linux-aarch64", "linux-64"):
        assert f'micromamba_platform="{platform}"' in script
    for package in (
        "quarto=1.7.31",
        "r-base=4.4.3",
        "r-knitr=1.49",
        "r-rmarkdown=2.29",
        "r-reticulate=1.40.0",
        "r-xfun=0.49",
    ):
        assert package in script
    assert "/.tools/" in (ROOT / ".gitignore").read_text(encoding="utf-8")
    assert "quarto-dev/quarto-actions" not in ci
    assert "r-lib/actions/setup-r" not in ci


@pytest.mark.slow
@pytest.mark.parametrize(
    "failure",
    [
        "setup",
        "python",
        "bash",
        "pipeline",
        "render",
        "validation",
        "cleanup",
        "expected-error",
        "missing-moto",
        "none",
    ],
)
def test_docs_build_failures__real_coordinator_fails_closed_and_releases_resources(tmp_path, failure):
    source = tmp_path / "source"
    docs = source / "docs"
    docs.mkdir(parents=True)
    for name in (
        "build.sh",
        "build-render.sh",
        "build-lib.sh",
        "build.py",
        "build-requirements.R",
        "build-bootstrap.qmd",
        "build-teardown.qmd",
        "_quarto.yml",
    ):
        shutil.copy2(ROOT / "docs" / name, docs / name)
    shutil.copy2(ROOT / "docs/moto_server_env.py", docs / "moto_server_env.py")
    (docs / "examples").mkdir()
    audit = tmp_path / "moto.json"
    teardown = docs / "build-teardown.qmd"
    teardown.write_text(
        teardown.read_text().replace(
            "docs_teardown",
            'if [[ -f "$DOCS_BUILD_WORK/moto/moto.json" ]]; then\n'
            '  cp "$DOCS_BUILD_WORK/moto/moto.json" "$DOCS_TEST_AUDIT"\nfi\n'
            "docs_teardown" + ("\nexit 43" if failure == "cleanup" else ""),
        )
    )
    if failure == "setup":
        bootstrap = docs / "build-bootstrap.qmd"
        bootstrap.write_text(bootstrap.read_text().replace("docs_bootstrap", "docs_bootstrap\nexit 41"))
    if failure == "missing-moto":
        library = docs / "build-lib.sh"
        library.write_text(library.read_text().replace("command -v moto_server", "command -v docs_missing_moto_server"))
    body = {
        "python": '```{python}\nraise RuntimeError("injected Python failure")\n```',
        "bash": '```{bash}\nfalse\nprintf "unreachable"\n```',
        "pipeline": '```{bash}\nfalse | true\nprintf "unreachable"\n```',
        "validation": "```python\nassert False\n```",
        "expected-error": (
            '```{python}\ntry:\n    pass\nexcept ValueError:\n    pass\nelse:\n'
            '    raise AssertionError("expected ValueError")\n```'
        ),
    }.get(
        failure,
        "```{python}\n#| include: false\nimport os\nfrom pathlib import Path\n"
        'root = Path(os.environ["DOCS_BUILD_WORK"])\n'
        'assert Path(os.environ["DML_CONFIG_HOME"]).is_relative_to(root)\n'
        'assert "DML_PROJECT_HOME" not in os.environ\n'
        'assert "AWS_ENDPOINT_URL_S3" not in os.environ\n'
        'assert "AWS_PROFILE" not in os.environ\n'
        'Path(os.environ["DOCS_TEST_MARKER"]).touch()\n```\n'
        '```{bash}\n#| output: false\ntest -f "$DOCS_TEST_MARKER"\n```',
    )
    (docs / "probe.qmd").write_text("---\ntitle: Probe\nengine: knitr\n---\n\n" + body + "\n")
    if failure == "none":
        # The dependency creates state in the fresh shared workspace. The
        # alphabetically earlier dependent proves that the build uses graph
        # order rather than filename order.
        project_setup = (
            '```{bash}\n#| output: false\ntest "$PWD" = "$DOCS_WORKSPACE_ROOT"\n'
            "test ! -e research-demo/page-sentinel\n"
            "mkdir research-demo\ncd research-demo\ndml init\ntouch page-sentinel\n```\n"
        )
        (docs / "probe.qmd").write_text(
            "---\ntitle: Probe\nengine: knitr\n---\n\n" + body + "\n" + project_setup,
            encoding="utf-8",
        )
        dependent_probe = (
            "```{python}\n#| include: false\n"
            "import os\nfrom pathlib import Path\n"
            "import boto3\nfrom daggerml import Dml\n"
            'workspace = Path(os.environ["DOCS_WORKSPACE_ROOT"])\n'
            'project = workspace / "research-demo"\n'
            "assert Path.cwd() == project\n"
            'assert Path(os.environ["DML_PROJECT_HOME"]) == project\n'
            'assert Path(os.environ["DML_CONFIG_HOME"]) == Path(os.environ["DOCS_BUILD_WORK"]) / "config"\n'
            'assert Path("page-sentinel").is_file()\n'
            "Dml()\n"
            's3 = boto3.client("s3")\n'
            's3.put_object(Bucket="daggerml-docs", Key="dependent", Body=b"fixture")\n'
            'assert s3.get_object(Bucket="daggerml-docs", Key="dependent")["Body"].read() == b"fixture"\n'
            'logs = boto3.client("logs")\n'
            'logs.create_log_group(logGroupName="dependent")\n'
            'assert logs.describe_log_groups(logGroupNamePrefix="dependent")["logGroups"]\n'
            "```\n"
            '```{bash}\n#| output: false\ntest "$PWD" = "$DML_PROJECT_HOME"\n'
            'test -f page-sentinel\ntest -n "$AWS_ENDPOINT_URL"\n```\n'
        )
        (docs / "aaa-dependent.qmd").write_text(
            "---\ntitle: Dependent\nengine: knitr\ndml-project-home: research-demo\n"
            "depends-on: probe\n---\n\n" + dependent_probe,
            encoding="utf-8",
        )
    if failure == "render":
        coordinator = docs / "build.py"
        coordinator.write_text(
            coordinator.read_text().replace(
                "prepare(args.work)",
                'prepare(args.work)\n            (args.work / "probe.qmd").write_text('
                '"{{< include missing-file.qmd >}}")',
            )
        )
    user_config = tmp_path / "user-config"
    user_config.mkdir()
    (user_config / "sentinel").write_text("untouched")
    marker = tmp_path / "executed"
    environment = {
        **os.environ,
        "DOCS_PYTHON": sys.executable,
        "DOCS_TEST_AUDIT": str(audit),
        "DOCS_TEST_MARKER": str(marker),
        "DML_CONFIG_HOME": str(user_config),
        "DML_PROJECT_HOME": str(user_config),
        "AWS_PROFILE": "must-not-use",
        "AWS_ENDPOINT_URL_S3": "https://must-not-use.invalid",
        "TMPDIR": str(tmp_path),
        "PYTHONFAULTHANDLER": "1",
    }
    (docs / "build-staging").mkdir()
    (docs / "build-staging/manifest.json").write_text('{"stale": true}')
    for _ in range(2 if failure == "none" else 1):
        marker.unlink(missing_ok=True)
        result = subprocess.run(
            ["bash", str(docs / "build-render.sh")], env=environment, text=True, capture_output=True, timeout=300
        )
        diagnostic = result.stdout + result.stderr
        assert not any(
            crash in diagnostic.lower() for crash in ("segmentation fault", "fatal python error", "caught segfault")
        ), diagnostic
        assert (result.returncode == 0) == (failure == "none"), diagnostic
        if failure == "none":
            assert marker.is_file()
    assert (user_config / "sentinel").read_text() == "untouched"
    assert list(user_config.iterdir()) == [user_config / "sentinel"]
    assert not list(tmp_path.glob("daggerml-docs.*"))
    if failure not in {"validation", "missing-moto"}:
        endpoint = urlparse(json.loads(audit.read_text())["endpoint"])
        with socket.socket() as connection:
            assert connection.connect_ex((endpoint.hostname, endpoint.port)) != 0
    if failure != "none":
        assert not (docs / "build-staging").exists()
        if failure == "missing-moto":
            assert "docs fixtures require moto_server" in diagnostic
        if failure in {"python", "bash", "pipeline", "expected-error"}:
            assert "probe.qmd" in result.stderr
            assert "unnamed-chunk" in result.stderr
    else:
        assert marker.is_file()
        assert (docs / "build-staging/manifest.json").is_file()


def _wheel_contents(wheel):
    with zipfile.ZipFile(wheel) as archive:
        return set(archive.namelist())


def _wheel_metadata(wheel):
    with zipfile.ZipFile(wheel) as archive:
        metadata = next(name for name in archive.namelist() if name.endswith(".dist-info/METADATA"))
        return message_from_bytes(archive.read(metadata))


@pytest.mark.slow
def test_docs_build_008__wheels_package_and_serve_completed_docs_without_build_tools(tmp_path):
    source = tmp_path / "source"
    shutil.copytree(
        ROOT,
        source,
        ignore=shutil.ignore_patterns(
            ".git", ".tools", ".venv", "__pycache__", "build", "build-staging", "dist", "static"
        ),
    )
    static = source / "src/daggerml/dashboard/static"
    static_docs = static / "docs"
    fragment = static_docs / "fragments/start-here/dags.html"
    asset = static_docs / "assets/start-here/dags_files/libs/quarto-html/quarto.js"
    for path in (fragment, asset):
        path.parent.mkdir(parents=True, exist_ok=True)
    (static / "assets").mkdir(parents=True, exist_ok=True)
    (static / "index.html").write_text("dashboard", encoding="utf-8")
    (static / "assets/app.js").write_text("// dashboard", encoding="utf-8")
    fragment.write_text("<main><h1>Analysis report</h1></main>", encoding="utf-8")
    asset.write_text("// generated asset", encoding="utf-8")
    manifest = {
        "pages": [
            {
                "id": "start-here/dags",
                "fragment": "fragments/start-here/dags.html",
                "headings": [],
            }
        ],
        "assets": ["assets/start-here/dags_files/libs/quarto-html/quarto.js"],
    }
    (static_docs / "manifest.json").write_text(json.dumps(manifest), encoding="utf-8")
    build_environment = {**os.environ, "SETUPTOOLS_SCM_PRETEND_VERSION_FOR_DAGGERML": "0.0.0"}

    artifacts = tmp_path / "artifacts"
    subprocess.run(
        ["uv", "build", "--wheel", "--sdist", "--out-dir", str(artifacts), str(source)],
        check=True,
        cwd=tmp_path,
        env=build_environment,
    )
    wheel = next(artifacts.glob("*.whl"))
    sdist = next(artifacts.glob("*.tar.gz"))
    from_sdist = tmp_path / "from-sdist"
    subprocess.run(
        ["uv", "build", "--wheel", "--out-dir", str(from_sdist), str(sdist)],
        check=True,
        cwd=tmp_path,
        env=build_environment,
    )
    sdist_wheel = next(from_sdist.glob("*.whl"))

    expected_files = {
        "daggerml/dashboard/static/docs/manifest.json",
        "daggerml/dashboard/static/docs/fragments/start-here/dags.html",
        "daggerml/dashboard/static/docs/assets/start-here/dags_files/libs/quarto-html/quarto.js",
    }
    for distribution in (wheel, sdist_wheel):
        assert expected_files <= _wheel_contents(distribution)
        metadata = _wheel_metadata(distribution)
        assert metadata.get_all("Requires-Dist") == [
            "boto3",
            'rich; extra == "terminal"',
            'fastapi<1,>=0.115; extra == "dashboard"',
            'uvicorn<1,>=0.32; extra == "dashboard"',
        ]
        assert metadata.get_all("Provides-Extra") == ["terminal", "dashboard"]

    with tarfile.open(sdist) as archive:
        contents = set(archive.getnames())
    assert {f"{sdist.stem.removesuffix('.tar')}/src/{path}" for path in expected_files} <= contents

    environment_vars = {key: value for key, value in os.environ.items() if key != "PYTHONPATH"}
    check_installed = source / "tests/distribution/check_installed_docs.py"
    for index, distribution_artifact in enumerate((wheel, sdist_wheel)):
        environment = tmp_path / f"installed-{index}"
        subprocess.run([sys.executable, "-m", "venv", str(environment)], check=True)
        python = environment / "bin/python"
        subprocess.run(
            [python, "-m", "pip", "install", f"{distribution_artifact}[dashboard]", "httpx"], check=True
        )
        subprocess.run([python, "-I", check_installed], check=True, cwd=tmp_path, env=environment_vars)
