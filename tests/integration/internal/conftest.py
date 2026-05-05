from __future__ import annotations

import shutil
import tempfile
from pathlib import Path

import pytest

from daggerml._internal._db import DmlDbEnv, DmlDbMapFullError
from daggerml._internal.ops.base_ops import BaseOps
from daggerml._internal.types import NAMESPACES


class TmpEnv(DmlDbEnv):
    def clear_all(self):
        while True:
            try:
                with self.tx(readonly=False) as txn:
                    for ns in NAMESPACES:
                        for obj, _ in txn.iter(ns):
                            txn.delete(obj)
                db_path = Path(self.path)
                repo_root = db_path.parent.parent if db_path.name == "db" and db_path.parent.name == ".dml" else db_path
                shutil.rmtree(repo_root / ".dml", ignore_errors=True)
                db_path.mkdir(parents=True, exist_ok=True)
                return
            except DmlDbMapFullError:
                self.resize(self.get_size() * 2)


@pytest.fixture(scope="module")
def temp_bo():
    with tempfile.TemporaryDirectory() as temp_dir:
        db_path = Path(temp_dir) / ".dml" / "db"
        db_path.mkdir(parents=True, exist_ok=True)
        db_env = TmpEnv.create(str(db_path), namespaces=sorted(NAMESPACES))
        try:
            yield BaseOps(db_env)
        finally:
            db_env.clear_all()
            db_env.close()
