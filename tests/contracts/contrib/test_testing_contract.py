from __future__ import annotations

import os

from daggerml import Uri
from daggerml.contrib import api
from daggerml.contrib.testing import MockNode, defunkify


def test_value_node_returns_wrapped_scalar():
    node = MockNode(7)

    assert node.value() == 7


def test_value_node_returns_wrapped_uri():
    uri = Uri("s3://bucket/path")
    node = MockNode(uri)

    assert node.value() is uri


def test_value_node_supports_funkify_style_author_code():
    def fn(dag, x, y):
        return x.value() + y.value()

    assert fn(None, MockNode(2), MockNode(5)) == 7


def test_mock_node_alias_matches_value_node_behavior():
    node = MockNode(7)

    assert isinstance(node, MockNode)
    assert node.value() == 7


def test_defunkify_wraps_plain_args_and_kwargs():
    @api.funkify
    def fn(dag, x, *, y):
        return x.value() + y.value()

    call = defunkify(fn)

    assert call(None, 2, y=5) == 7


def test_defunkify_unwraps_nested_delayed_runnable():
    @api.funkify(uri="docker", image="repo/name")
    @api.funkify
    def fn(dag, x, y):
        return x.value() + y.value()

    call = defunkify(fn)

    assert call(None, MockNode(2), 5) == 7


def test_defunkify_runs_in_isolated_workdir(tmp_path):
    touched = []

    @api.funkify
    def fn(dag):
        touched.append(os.getcwd())
        open("artifact.txt", "w").write("ok")
        return 1

    cwd = os.getcwd()
    call = defunkify(fn)

    assert call(None) == 1
    assert os.getcwd() == cwd
    assert touched[0] != cwd
    assert not (tmp_path / "artifact.txt").exists()
    assert not os.path.exists("artifact.txt")
