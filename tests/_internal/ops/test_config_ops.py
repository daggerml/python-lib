from __future__ import annotations

import pytest

from daggerml._internal.ops.config import SCOPE_GLOBAL, SCOPE_LOCAL, ConfigOps
from daggerml._internal.types import DmlRepoError


def test_config_ops_set_get_local_project_uri(tmp_path):
    ops = ConfigOps(project_home=str(tmp_path), config_home=str(tmp_path / "cfg"))
    ops.set("project.uri", ["dml://alice/demo#main"], scope=SCOPE_LOCAL)
    assert ops.get("project.uri", scope=SCOPE_LOCAL) == "dml://alice/demo#main"


def test_config_ops_set_get_global_user(tmp_path):
    ops = ConfigOps(project_home=str(tmp_path), config_home=str(tmp_path / "cfg"))
    ops.set("user", ["alice@host"], scope=SCOPE_GLOBAL)
    assert ops.get("user", scope=SCOPE_GLOBAL) == "alice@host"


def test_config_ops_validates_scope_restrictions(tmp_path):
    ops = ConfigOps(project_home=str(tmp_path), config_home=str(tmp_path / "cfg"))
    with pytest.raises(DmlRepoError, match="not valid in global scope"):
        ops.set("project.uri", ["dml://alice/demo#main"], scope=SCOPE_GLOBAL)
    with pytest.raises(DmlRepoError, match="not valid in local scope"):
        ops.set("user", ["alice@host"], scope=SCOPE_LOCAL)


def test_config_ops_hooks_accept_multiple_values(tmp_path):
    ops = ConfigOps(project_home=str(tmp_path), config_home=str(tmp_path / "cfg"))
    ops.set("hooks.post-init", ["echo one", "echo two"], scope=SCOPE_GLOBAL)
    assert ops.get("hooks.post-init", scope=SCOPE_GLOBAL) == ["echo one", "echo two"]
