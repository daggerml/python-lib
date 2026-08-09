from __future__ import annotations

import pytest
from hypothesis import given, settings

from daggerml._core.db import Ref
from daggerml._core.revision import Revision
from tests._core.strategies import revision_selectors


@given(revision_selectors.filter(lambda selector: not selector.startswith("dml://")))
@settings(max_examples=50, deadline=None)
def test_accepted_selectors_parse_and_stringify(selector: str) -> None:
    revision = Revision.from_str(selector)

    expected = f"commit:{selector}" if len(selector) == 64 and selector[:1] in "0123456789abcdef" else selector
    assert str(revision) == expected
    assert revision.kind in {"head", "name", "commit"}


@pytest.mark.parametrize(
    ("selector", "kind"),
    [
        ("HEAD", "head"),
        ("HEAD~3", "head"),
        ("commit:" + "a" * 64, "commit"),
        ("feature/x", "name"),
    ],
)
def test_representative_selector_kinds(selector: str, kind: str) -> None:
    assert Revision.from_str(selector).kind == kind


@pytest.mark.parametrize("selector", ["", "HEAD~-1", "HEAD~x", "bad//name", "origin/main", "dml://aa/bb#main"])
def test_malformed_selectors_reject(selector: str) -> None:
    with pytest.raises(ValueError):
        Revision.from_str(selector)


def test_explicit_commit_requires_commit_namespace() -> None:
    with pytest.raises(ValueError, match="Expected commit ref"):
        Revision(commit=Ref("dag:x"))
