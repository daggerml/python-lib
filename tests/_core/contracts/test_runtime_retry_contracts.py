from __future__ import annotations

from contextlib import nullcontext
from types import SimpleNamespace

import pytest

import daggerml._core.dml as dml_mod
from daggerml._core.db import DmlDbMapFullError, Ref


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
