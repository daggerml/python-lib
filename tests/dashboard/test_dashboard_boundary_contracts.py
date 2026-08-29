import ast
from pathlib import Path


def test_dash_boundary_001__dashboard_avoids_core_submodules_and_private_dml_state():
    dashboard = Path(__file__).parents[2] / "src" / "daggerml" / "dashboard"
    for path in dashboard.glob("*.py"):
        tree = ast.parse(path.read_text(encoding="utf-8"))
        for node in ast.walk(tree):
            if isinstance(node, ast.ImportFrom) and node.module and node.module.startswith("daggerml._core."):
                raise AssertionError(f"{path} imports private core module {node.module}")
            if isinstance(node, ast.Attribute) and node.attr.startswith("_"):
                if isinstance(node.value, ast.Name) and node.value.id == "dml":
                    raise AssertionError(f"{path} accesses dml.{node.attr}")
                if isinstance(node.value, ast.Attribute) and node.value.attr == "dml":
                    raise AssertionError(f"{path} accesses self.dml.{node.attr}")
