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
                shutil.rmtree(Path(self.path) / ".dml", ignore_errors=True)
                return
            except DmlDbMapFullError:
                self.resize(self.get_size() * 2)


@pytest.fixture(scope="module")
def temp_bo():
    with tempfile.TemporaryDirectory() as temp_dir:
        db_env = TmpEnv.create(temp_dir, namespaces=sorted(NAMESPACES))
        try:
            yield BaseOps(db_env)
        finally:
            db_env.clear_all()
            db_env.close()
