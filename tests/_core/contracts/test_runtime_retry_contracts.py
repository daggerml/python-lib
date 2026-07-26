from __future__ import annotations

from contextlib import nullcontext
from types import SimpleNamespace

import pytest

import daggerml._core.dml as dml_mod
from daggerml._core.db import DmlDbMapFullError, Ref
from daggerml._core.s3_cas import CasItemConflict


def test_runtime_commit_retries_full_orchestration_after_cas_conflict(monkeypatch) -> None:
    calls = {"commit": 0, "head": 0, "merge": 0, "update": 0}
    dag_ref = Ref("dag:" + "d" * 64)
    staged_commit = Ref("commit:" + "1" * 64)
    merged_commit = Ref("commit:" + "2" * 64)

    class FakeIndexOps:
        def commit(self, index, value, *, author, message, name, db):
            calls["commit"] += 1
            return dag_ref, staged_commit

    class FakeHead:
        def lock(self):
            return nullcontext()

        def get_head(self):
            calls["head"] += 1
            return {"commit": Ref("commit:" + "0" * 64), "branch": "main"}

        def update_local_ref(self, branch, commit_ref):
            calls["update"] += 1
            assert branch == "main"
            assert commit_ref == merged_commit

        def write_detached_head(self, commit_ref):
            raise AssertionError("commit should stay attached in this test")

    class FakeCommitOps:
        def merge(self, current, incoming, *, user, db):
            calls["merge"] += 1
            assert incoming == staged_commit
            if calls["merge"] == 1:
                raise CasItemConflict("merge cas conflict")
            return merged_commit

    fake_dml = SimpleNamespace(_db=object(), _config=SimpleNamespace(user="tester"))
    runtime = dml_mod._RuntimeNamespace(fake_dml)

    monkeypatch.setattr(dml_mod, "_index_ops", lambda dml: FakeIndexOps())
    monkeypatch.setattr(dml_mod, "_head_ops", lambda dml: FakeHead())
    monkeypatch.setattr(dml_mod, "CommitOps", FakeCommitOps)

    result = runtime.commit(Ref("index:idx"), Ref("node-literal:n"), name="dag")

    assert result == dag_ref
    assert calls == {"commit": 2, "head": 2, "merge": 2, "update": 1}


def test_runtime_commit_does_not_retry_map_full_error(monkeypatch) -> None:
    calls = {"commit": 0, "merge": 0}

    class FakeIndexOps:
        def commit(self, index, value, *, author, message, name, db):
            calls["commit"] += 1
            return Ref("dag:" + "d" * 64), Ref("commit:" + "1" * 64)

    class FakeHead:
        def lock(self):
            return nullcontext()

        def get_head(self):
            return {"commit": Ref("commit:" + "0" * 64), "branch": "main"}

        def update_local_ref(self, branch, commit_ref):
            raise AssertionError("should not update after map-full")

        def write_detached_head(self, commit_ref):
            raise AssertionError("should not detach after map-full")

    class FakeCommitOps:
        def merge(self, current, incoming, *, user, db):
            calls["merge"] += 1
            raise DmlDbMapFullError()

    fake_dml = SimpleNamespace(_db=object(), _config=SimpleNamespace(user="tester"))
    runtime = dml_mod._RuntimeNamespace(fake_dml)

    monkeypatch.setattr(dml_mod, "_index_ops", lambda dml: FakeIndexOps())
    monkeypatch.setattr(dml_mod, "_head_ops", lambda dml: FakeHead())
    monkeypatch.setattr(dml_mod, "CommitOps", FakeCommitOps)

    with pytest.raises(DmlDbMapFullError):
        runtime.commit(Ref("index:idx"), Ref("node-literal:n"), name="dag")

    assert calls == {"commit": 1, "merge": 1}
