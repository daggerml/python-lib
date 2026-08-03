from __future__ import annotations

import pytest

from daggerml._core import Dml
from daggerml._core.db import Ref
from daggerml._core.head import Head
from daggerml._core.types import DmlRepoError


def test_remote_lifecycle_rejects_deletion_while_a_branch_tracks_it(tmp_path) -> None:
    dml = Dml.init(str(tmp_path))

    assert dml.remote.add("research", "dml://acme/research") == "research"
    assert dml.remote.list() == {"research": "dml://acme/research"}
    head = Head(str(tmp_path))
    head.create_local_ref("feature", Ref("commit:" + "a" * 64))
    head.set_upstream("feature", "research", "feature")

    with pytest.raises(DmlRepoError, match="tracked by a local branch"):
        dml.remote.delete("research")

    dml.branch.delete("feature")
    dml.remote.delete("research")
    assert dml.remote.list() == {}


def test_set_upstream_requires_an_attached_branch_and_known_remote(tmp_path) -> None:
    dml = Dml.init(str(tmp_path))
    dml.remote.add("research", "dml://acme/research")

    assert dml.branch.set_upstream("research/main") == "research/main"
    assert dml.status()["upstream"] == "research/main"
    with pytest.raises(DmlRepoError, match="Unknown remote"):
        dml.branch.set_upstream("unknown/main")
