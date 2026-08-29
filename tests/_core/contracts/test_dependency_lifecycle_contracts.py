from __future__ import annotations

import json

import pytest

from daggerml._core import Dml
from daggerml._core.head import Head
from daggerml._core.types import DmlRepoError


def test_dependency_lifecycle_uses_isolated_strict_config(tmp_path) -> None:
    dml = Dml.init(str(tmp_path))

    assert dml.dep.add("models", "s3://bucket/models/") == "models"
    assert dml.dep.list() == {"models": "s3://bucket/models"}
    config_path = Head(str(tmp_path)).dependency_config_path("models")
    assert json.loads(config_path.read_text()) == {"backend": "s3", "root": "s3://bucket/models"}

    dml.dep.delete("models")
    assert dml.dep.list() == {}


def test_dependency_lifecycle_rejects_invalid_and_unknown_names(tmp_path) -> None:
    dml = Dml.init(str(tmp_path))

    with pytest.raises(ValueError):
        dml.dep.add("bad/name", "s3://bucket/models")
    with pytest.raises(DmlRepoError, match="does not exist"):
        dml.dep.delete("models")
