from __future__ import annotations

import importlib.util
import json
import os
import shutil
import subprocess
import sys
import tarfile
import tomllib
import zipfile
from email import message_from_bytes
from pathlib import Path

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
