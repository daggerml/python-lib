from __future__ import annotations

import ast
from pathlib import Path


def test_db_stub_exposes_only_typed_transaction_operations() -> None:
    path = Path(__file__).parents[3] / "src" / "daggerml" / "_core" / "db.pyi"
    module = ast.parse(path.read_text(encoding="utf-8"))
    classes = {node.name: node for node in module.body if isinstance(node, ast.ClassDef)}
    db_methods = {node.name for node in classes["DmlDb"].body if isinstance(node, ast.FunctionDef)}
    txn_methods = {node.name: node for node in classes["DmlDbTxn"].body if isinstance(node, ast.FunctionDef)}

    assert "write_with_growth" in db_methods
    assert "call_with_resize" not in db_methods
    assert [arg.arg for arg in txn_methods["get"].args.args] == ["self", "key"]
    assert [arg.arg for arg in txn_methods["put"].args.kwonlyargs] == ["ns", "to", "no_overwrite"]
