from __future__ import annotations

import daggerml._core.dml as dml_mod
from daggerml._core.dml import Dml
from tests._core.helpers import local_index_ops, run_parallel


def test_non_conflicting_names_are_merged(tmp_path, monkeypatch) -> None:
    monkeypatch.setenv("DML_DEFAULT_DB_MAP_SIZE_MAX", str(64 * 1024 * 1024))
    Dml.init(str(tmp_path), user="tester", remote_root="s3://bucket/root")
    ops = local_index_ops()
    monkeypatch.setattr(dml_mod, "_index_ops", lambda dml: ops)
    dml = Dml(str(tmp_path), remote_root="s3://bucket/root", user="tester")

    def commit_named(i: int):
        index = dml.runtime.create()
        node = dml.runtime.put_literal(index, i, name=f"n{i}")
        return dml.runtime.commit(index, node, name=f"dag{i}")

    commits = run_parallel(4, commit_named)

    assert len(set(commits)) == 4
    shown = dml.show("HEAD")
    assert set(shown["dags"]) == {f"dag{i}" for i in range(4)}
