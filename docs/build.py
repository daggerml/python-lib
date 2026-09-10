"""Prepare, validate, and stage executable documentation without build dependencies."""

from __future__ import annotations

import argparse
import ast
import html
import json
import re
import shutil
import sys
import zipfile
from pathlib import Path
from urllib.parse import unquote, urlsplit

import yaml

ROOT = Path(__file__).resolve().parent
SOURCE_MARKER = re.compile(r"\{\{<\s*dml-source\s+([^\s>]+)\s*>\}\}")
FENCE = re.compile(r"^ {0,3}(?P<delimiter>`{3,}|~{3,})(?P<info>[^\n]*)$", re.MULTILINE)
POLICY = {"eval": True, "cache": False, "freeze": False, "error": False}
SECRET = re.compile(r"(DOCS_BUILD_WORK|daggerml-docs/artifacts|moto_server)")


def qmd_files(root: Path) -> list[Path]:
    return sorted(
        path
        for path in root.rglob("*.qmd")
        if not {"build-staging", "_build", ".quarto"}.intersection(path.relative_to(root).parts)
    )


def project_root(qmd: Path) -> Path:
    for parent in (qmd.parent, *qmd.parents):
        if (parent / "_quarto.yml").is_file():
            return parent.resolve()
    return ROOT.resolve()


def source_path(qmd: Path, value: str) -> Path:
    path = (qmd.parent / value).resolve()
    examples = (project_root(qmd) / "examples").resolve()
    if not path.is_relative_to(examples) or path.suffix != ".py" or not path.is_file():
        raise ValueError(f"{qmd}: dml-source must name a Python file beneath docs/examples: {value}")
    return path


def validate(root: Path = ROOT) -> None:
    errors: list[str] = []

    def policy(value: object, location: str) -> None:
        if not isinstance(value, dict):
            return
        for key, setting in value.items():
            if key in POLICY and setting is not POLICY[key]:
                errors.append(f"{location}: execution policy forbids {key}: {str(setting).lower()}")
            if key == "engine" and setting != "knitr":
                errors.append(f"{location}: execution policy requires engine: knitr")
            if key == "execute" and not isinstance(setting, dict):
                errors.append(f"{location}: execute must contain explicit execution options")
            if key in {"engine.path", "engine.opts"} and setting != {"bash": "-euo pipefail"}:
                errors.append(f"{location}: execution engine overrides are prohibited")
            policy(setting, location)

    def python_policy(body: str, location: str) -> None:
        try:
            tree = ast.parse(body)
        except SyntaxError as exc:
            errors.append(f"{location}: invalid executable Python: {exc}")
            return
        for node in ast.walk(tree):
            if isinstance(node, (ast.Import, ast.ImportFrom)):
                modules = [node.module or ""] if isinstance(node, ast.ImportFrom) else [a.name for a in node.names]
                if any(module.split(".")[0] in {"subprocess", "sh", "pexpect", "plumbum"} for module in modules):
                    errors.append(f"{location}: Python shell wrappers are prohibited")
                if isinstance(node, ast.ImportFrom) and node.module == "os":
                    if any(a.name in {"system", "popen"} or a.name.startswith(("exec", "spawn")) for a in node.names):
                        errors.append(f"{location}: Python shell wrappers are prohibited")
            if isinstance(node, ast.Attribute) and (
                node.attr in {"system", "popen", "get_ipython"} or node.attr.startswith(("spawn", "execv", "execl"))
            ):
                errors.append(f"{location}: Python shell wrappers are prohibited")

    for config in sorted(set(root.rglob("_quarto*.yml")) | set(root.rglob("_metadata.yml"))):
        if {"build-staging", "_build", ".quarto"}.intersection(config.relative_to(root).parts):
            continue
        policy(yaml.safe_load(config.read_text(encoding="utf-8")), str(config))
    for qmd in qmd_files(root):
        text = qmd.read_text(encoding="utf-8")
        frontmatter = re.match(r"\A---\s*\n(.*?)\n---\s*(?:\n|$)", text, re.DOTALL)
        if frontmatter:
            policy(yaml.safe_load(frontmatter.group(1)), str(qmd))
        for match in SOURCE_MARKER.finditer(text):
            try:
                source_path(qmd, match.group(1))
            except ValueError as exc:
                errors.append(str(exc))
        end = 0
        for match in FENCE.finditer(text):
            if match.start() < end:
                continue
            info = match.group("info").strip()
            delimiter = match.group("delimiter")
            closing = re.search(
                r"^ {0,3}" + re.escape(delimiter[0]) + "{" + str(len(delimiter)) + r",}\s*$",
                text[match.end() :],
                re.MULTILINE,
            )
            if closing is None:
                errors.append(f"{qmd}: unclosed code fence")
                continue
            body = text[match.end() : match.end() + closing.start()]
            end = match.end() + closing.end()
            location = f"{qmd}:{text.count(chr(10), 0, match.start()) + 1}"
            if info.startswith("{") and info.endswith("}"):
                language = re.split(r"[\s,]", info[1:-1])[0].lower()
                if language not in {"python", "bash", "r", "mermaid", "dot"}:
                    errors.append(f"{location}: unsupported executable engine {language!r}")
                options = "\n".join(re.findall(r"^\s*#\| ?(.*)$", body, re.MULTILINE))
                policy(yaml.safe_load(options), location)
                for key, value in re.findall(
                    r"\b(eval|cache|freeze|error)\s*=\s*([^,)\n]+)", info + (body if language == "r" else "")
                ):
                    expected = "TRUE" if POLICY[key] else "FALSE"
                    if value.strip() != expected:
                        errors.append(f"{location}: execution policy forbids {key}: {value.strip().lower()}")
                if re.search(r"\bengine\.(?:path|opts)\s*=", info + (body if language == "r" else "")):
                    errors.append(f"{location}: execution engine overrides are prohibited")
                if language == "python":
                    python_policy(body, location)
                continue
            language = info.split(maxsplit=1)[0].lower() if info else "untyped"
            prefix = text[: match.start()].rstrip()
            if language not in {"mermaid", "dot"} and not re.search(r"<!--\s*docs:pseudocode:\s*\S[^<>]*-->$", prefix):
                errors.append(f"{location}: static {language!r} fence must be executable or marked docs:pseudocode")
    for source in (root / "examples").rglob("*.py"):
        python_policy(source.read_text(encoding="utf-8"), str(source))
    if errors:
        raise ValueError("\n".join(errors))


def expand_sources(qmd: Path, text: str) -> str:
    def replace(match: re.Match[str]) -> str:
        source = source_path(qmd, match.group(1))
        relative = source.relative_to(project_root(qmd)).as_posix()
        displayed = html.escape(source.read_text(encoding="utf-8"))
        return (
            f'<div data-dml-source="{relative}"><pre><code class="language-python">{displayed}</code></pre></div>\n\n'
            "```{python}\n#| echo: false\n#| output: false\n"
            "import os\nimport runpy\nimport sys\nfrom pathlib import Path\n"
            f'source = Path(os.environ["DOCS_SOURCE_ROOT"]) / "{relative}"\n'
            "sys.path.insert(0, str(source.parent))\n"
            'runpy.run_path(str(source), run_name="__main__")\n```'
        )

    return SOURCE_MARKER.sub(replace, text)


def prepare(work: Path) -> None:
    if work.exists():
        shutil.rmtree(work)
    shutil.copytree(
        ROOT, work, ignore=shutil.ignore_patterns("_build", "build-staging", "__pycache__", ".quarto", "*_files")
    )
    for qmd in qmd_files(work):
        text = expand_sources(qmd, qmd.read_text(encoding="utf-8"))
        page = qmd.relative_to(work).with_suffix("").as_posix()
        setup = (
            "\n```{r}\n#| include: false\n"
            f'page_root <- file.path(Sys.getenv("DOCS_BUILD_WORK"), "pages", "{page}")\n'
            "dir.create(page_root, recursive = TRUE, showWarnings = FALSE)\n"
            'Sys.setenv(DML_CONFIG_HOME = file.path(page_root, "config"), '
            "DOCS_PAGE_ROOT = page_root)\n"
            "knitr::opts_knit$set(root.dir = page_root)\n```\n\n"
        )
        if SOURCE_MARKER.search(qmd.read_text(encoding="utf-8")):
            setup += (
                "```{python}\n#| include: false\n"
                "import os\nfrom daggerml import Dml\n"
                'os.environ["DML_PROJECT_HOME"] = os.environ["DOCS_PAGE_ROOT"]\n'
                'Dml.init(os.environ["DOCS_PAGE_ROOT"], user="docs")\n```\n\n'
            )
        frontmatter = re.match(r"\A---\s*\n.*?\n---\s*\n", text, re.DOTALL)
        position = frontmatter.end() if frontmatter else 0
        qmd.write_text(text[:position] + setup + text[position:], encoding="utf-8")
    for qmd in work.glob("build-*.qmd"):
        qmd.unlink()


def page_id(html_path: Path, render: Path) -> str:
    relative = html_path.relative_to(render).with_suffix("")
    parts = list(relative.parts)
    if parts[-1] == "index":
        parts.pop()
    return "/".join(parts) or "index"


def fragment(document: str) -> str:
    document = re.sub(r"<script\b[^>]*>.*?</script\s*>", "", document, flags=re.IGNORECASE | re.DOTALL)
    body = re.search(r"<main\b[^>]*>.*?</main\s*>", document, flags=re.IGNORECASE | re.DOTALL)
    if body is None:
        body = re.search(r"<body\b[^>]*>.*?</body\s*>", document, flags=re.IGNORECASE | re.DOTALL)
    return body.group(0) if body else document


def rewrite_urls(content: str, source: Path, render: Path) -> str:
    def replace(match: re.Match[str]) -> str:
        attribute, quote, value = match.groups()
        if value.startswith(("#", "/", "//")) or re.match(r"[a-z][a-z0-9+.-]*:", value, re.IGNORECASE):
            return match.group(0)
        path, separator, anchor = value.partition("#")
        repository_target = (ROOT / source.relative_to(render).parent / path).resolve()
        if (
            repository_target.is_relative_to(ROOT.parent)
            and not repository_target.is_relative_to(ROOT)
            and repository_target.exists()
        ):
            relative = repository_target.relative_to(ROOT.parent).as_posix()
            suffix = f"#{anchor}" if separator else ""
            return f"{attribute}={quote}https://github.com/daggerml/python-lib/blob/master/{relative}{suffix}{quote}"
        target = (source.parent / path).resolve()
        try:
            relative = target.relative_to(render)
        except ValueError:
            return match.group(0)
        if target.suffix in {".html", ".md", ".qmd"}:
            page = page_id(target.with_suffix(".html"), render)
            suffix = f"#{anchor}" if separator else ""
            return f"{attribute}={quote}/docs/{page if page != 'index' else ''}{suffix}{quote}"
        if target.is_file():
            return f"{attribute}={quote}/docs/static/assets/{relative.as_posix()}{quote}"
        return match.group(0)

    return re.sub(r"\b(href|src)=([\"'])([^\"']+)\2", replace, content)


def stage(render: Path, staging: Path, *, require_all: bool = False) -> None:
    render = render.resolve()
    staging = staging.resolve()
    if staging.exists():
        shutil.rmtree(staging)
    fragments = staging / "fragments"
    assets = staging / "assets"
    downloads = staging / "downloads"
    manifest: dict[str, object] = {"pages": [], "assets": [], "downloads": []}
    for path in render.rglob("*"):
        if path.is_file() and path.suffix.lower() in {
            ".png",
            ".jpg",
            ".jpeg",
            ".gif",
            ".svg",
            ".webp",
            ".css",
            ".js",
            ".woff",
            ".woff2",
            ".ttf",
            ".otf",
        }:
            target = assets / path.relative_to(render)
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(path, target)
            cast_assets = manifest["assets"]
            assert isinstance(cast_assets, list)
            cast_assets.append(target.relative_to(staging).as_posix())
    for source in sorted((ROOT / "examples").rglob("*.py")):
        target = downloads / source.relative_to(ROOT)
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(source, target)
        cast_downloads = manifest["downloads"]
        assert isinstance(cast_downloads, list)
        cast_downloads.append(target.relative_to(staging).as_posix())
    for example in sorted(path for path in (ROOT / "examples").iterdir() if path.is_dir()):
        bundle = downloads / "examples" / example.name / f"{example.name}.zip"
        with zipfile.ZipFile(bundle, "w", zipfile.ZIP_DEFLATED) as archive:
            for source in sorted(example.rglob("*.py")):
                archive.write(source, source.relative_to(example.parent))
        cast_downloads = manifest["downloads"]
        assert isinstance(cast_downloads, list)
        cast_downloads.append(bundle.relative_to(staging).as_posix())
    for path in sorted(render.rglob("*.html")):
        page = page_id(path, render)
        content = fragment(path.read_text(encoding="utf-8"))
        if page.startswith("examples/"):
            example = page.removeprefix("examples/")
            example_root = ROOT / "examples" / example

            def download_href(match: re.Match[str], example: str = example, example_root: Path = example_root) -> str:
                value = Path(match.group(2))
                source = (example_root / value).resolve()
                if source.suffix == ".py" and source.is_relative_to(example_root.resolve()) and source.is_file():
                    relative = source.relative_to(ROOT / "examples").as_posix()
                    return f'href="/docs/static/downloads/examples/{relative}"'
                if value == Path(f"{example}.zip"):
                    return f'href="/docs/static/downloads/examples/{example}/{example}.zip"'
                return match.group(0)

            content = re.sub(
                r'href=(["\'])([^"\']+\.(?:py|zip))\1',
                download_href,
                content,
            )
        content = rewrite_urls(content, path, render)
        if SECRET.search(content):
            raise ValueError(f"{path}: fixture configuration leaked into staged fragment")
        identifiers = set(re.findall(r'\bid=["\']([^"\']+)["\']', content))
        for anchor in re.findall(r'href=["\']#([^"\']+)["\']', content):
            if anchor not in identifiers:
                raise ValueError(f"{path}: link points to missing anchor #{anchor}")
        target = fragments / f"{page}.html"
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(content, encoding="utf-8")
        headings = re.findall(r"<h([1-6])[^>]*\bid=[\"']([^\"']+)[\"'][^>]*>(.*?)</h\1>", content, re.DOTALL)
        title = re.search(r"<h1[^>]*>(.*?)</h1>", content, re.DOTALL)
        cast_pages = manifest["pages"]
        assert isinstance(cast_pages, list)
        cast_pages.append(
            {
                "id": page,
                "fragment": target.relative_to(staging).as_posix(),
                "title": re.sub("<[^>]+>", "", title.group(1)).strip() if title else page.replace("/", " ").title(),
                "headings": [
                    {"level": int(level), "id": ident, "text": re.sub("<[^>]+>", "", text)}
                    for level, ident, text in headings
                ],
            }
        )
    if require_all:
        expected = {
            page_id(path.with_suffix(".html"), ROOT) for path in qmd_files(ROOT) if not path.name.startswith("build-")
        }
        actual = {page_id(path, fragments) for path in fragments.rglob("*.html")}
        if expected != actual:
            raise ValueError(
                f"rendered page inventory mismatch: missing={sorted(expected - actual)}, "
                f"extra={sorted(actual - expected)}"
            )
        for path in fragments.rglob("*.html"):
            content = path.read_text(encoding="utf-8")
            for value in re.findall(r'(?:href|src)=["\']([^"\']+)["\']', content):
                url = urlsplit(html.unescape(value))
                if url.scheme or url.netloc or not url.path:
                    continue
                if url.path.startswith("/docs/static/"):
                    target = (staging / unquote(url.path.removeprefix("/docs/static/"))).resolve()
                    if not target.is_relative_to(staging.resolve()) or not target.is_file():
                        raise ValueError(f"{path}: missing static asset {value}")
                elif url.path.startswith("/docs/"):
                    target = fragments / (unquote(url.path.removeprefix("/docs/")).rstrip("/") or "index")
                    target = target.with_suffix(".html")
                    if not target.is_file():
                        raise ValueError(f"{path}: missing page {value}")
                    if url.fragment and unquote(url.fragment) not in re.findall(
                        r'\bid=["\']([^"\']+)["\']', target.read_text(encoding="utf-8")
                    ):
                        raise ValueError(f"{path}: missing heading {value}")
                else:
                    raise ValueError(f"{path}: unresolved internal URL {value}")
    (staging / "manifest.json").write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("validate")
    prepare_parser = commands.add_parser("prepare")
    prepare_parser.add_argument("--work", type=Path, required=True)
    stage_parser = commands.add_parser("stage")
    stage_parser.add_argument("--render", type=Path, required=True)
    stage_parser.add_argument("--staging", type=Path, required=True)
    args = parser.parse_args()
    try:
        if args.command == "validate":
            validate()
        elif args.command == "prepare":
            prepare(args.work)
        else:
            stage(args.render, args.staging, require_all=True)
    except (OSError, ValueError, yaml.YAMLError) as exc:
        print(f"documentation {args.command} failed: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
