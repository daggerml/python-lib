from __future__ import annotations

from daggerml._core.dml import Dml
from daggerml._core.head import Head
from tests._core.helpers import run_parallel


def test_concurrent_init_leaves_coherent_repo(tmp_path, monkeypatch) -> None:
    monkeypatch.setenv("DML_DEFAULT_DB_MAP_SIZE_MAX", str(64 * 1024 * 1024))
    results = run_parallel(20, lambda _: Dml.init(str(tmp_path), user="tester", remote_root="s3://bucket/root"))

    head_info = Head(str(tmp_path)).get_head()
    status = Dml(str(tmp_path), remote_root="s3://bucket/root", user="tester").status()

    assert all(isinstance(result, Dml) for result in results)
    assert all(result.status() == status for result in results)
    assert head_info["mode"] == "attached"
    assert head_info["branch"] == "main"
    assert head_info["commit"] is None
    assert status["branches"] == []
    assert status["num_indexes"] == 0
