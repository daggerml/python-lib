import ast
from importlib.util import resolve_name
from pathlib import Path

from daggerml._core import validate_adapter_response

SOURCE_ROOT = Path(__file__).parents[2] / "src" / "daggerml"


def _module_name(path: Path) -> str:
    relative = path.relative_to(SOURCE_ROOT)
    parts = relative.parts[:-1] if relative.name == "__init__.py" else (*relative.parts[:-1], relative.stem)
    return ".".join(("daggerml", *parts))


def _imported_module(node: ast.ImportFrom, importer: str, *, is_package: bool) -> str:
    if node.level == 0:
        return node.module or ""
    package = importer if is_package else importer.rpartition(".")[0]
    return resolve_name(f"{'.' * node.level}{node.module or ''}", package)


def test_core_boundary_001__adapter_response_validator_is_exported():
    response = {"status": "success", "error": None}

    assert validate_adapter_response(response) == response


def test_core_boundary_002__non_core_modules_import_only_the_core_facade():
    violations = []
    for path in sorted(SOURCE_ROOT.rglob("*.py")):
        importer = _module_name(path)
        if importer == "daggerml._core" or importer.startswith("daggerml._core."):
            continue
        for node in ast.walk(ast.parse(path.read_text(encoding="utf-8"), filename=str(path))):
            imported = []
            if isinstance(node, ast.Import):
                imported = [alias.name for alias in node.names]
            elif isinstance(node, ast.ImportFrom):
                imported = [_imported_module(node, importer, is_package=path.name == "__init__.py")]
            for module in imported:
                if module.startswith("daggerml._core."):
                    violations.append(f"{path.relative_to(SOURCE_ROOT)}:{node.lineno}: {module}")

    assert violations == [], "Forbidden daggerml._core submodule imports:\n" + "\n".join(violations)
