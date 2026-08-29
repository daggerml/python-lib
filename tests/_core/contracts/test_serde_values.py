from __future__ import annotations

import math

import pytest
from hypothesis import given, settings

from daggerml._core.db import Ref
from daggerml._core.serde import dml_dumps, dml_loads
from daggerml._core.types import Error, Runnable, Uri
from tests._core.strategies import serde_values


@given(serde_values())
@settings(max_examples=50, deadline=None)
def test_bounded_supported_values_round_trip(value) -> None:
    assert dml_loads(dml_dumps(value)) == value


def test_nested_error_ref_uri_runnable_round_trip() -> None:
    value = Runnable(
        target=Uri("file:///tool.py"),
        sub=Runnable(target=Uri("s3://bucket/sub")),
        kwargs={"ref": Ref("datum-scalar:x"), "error": Error("boom", "adapter", "runtime", [])},
        adapter="adapter",
    )

    assert dml_loads(dml_dumps(value)) == value


@pytest.mark.parametrize("value", [math.inf, -math.inf, math.nan, {1: "bad"}, object()])
def test_unsupported_values_reject(value) -> None:
    with pytest.raises(TypeError):
        dml_dumps(value)


@pytest.mark.parametrize(
    "payload",
    [
        "{}",
        "[]",
        "[\"unknown\", 1]",
        "[\"scalar\", {}]",
        "[\"ref\", 1]",
        "[\"runnable\", {\"target\": [\"scalar\", \"bad\"], \"sub\": [\"scalar\", null], "
        "\"kwargs\": [\"dict\", {}], \"adapter\": [\"scalar\", \"\"]}]",
    ],
)
def test_malformed_envelopes_reject(payload: str) -> None:
    with pytest.raises((TypeError, ValueError, KeyError)):
        dml_loads(payload)
