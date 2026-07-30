from __future__ import annotations

from collections import UserDict
from collections.abc import Sequence
from dataclasses import dataclass

import pytest

import daggerml.api as api
from daggerml._core import DmlRepoError, Error, Runnable, Uri
from daggerml.api import MiscPyTypeCodec, apply_codecs


class CustomSequence(Sequence):
    def __init__(self, values):
        self._values = values

    def __getitem__(self, index):
        return self._values[index]

    def __len__(self):
        return len(self._values)


class MatchesTypeCodec:
    def __init__(self, type_, result=None, error=None):
        self.type_ = type_
        self.result = result
        self.error = error

    def can_encode(self, value):
        if self.error is not None:
            raise self.error
        return isinstance(value, self.type_)

    def encode(self, value, dag):
        if self.error is not None:
            raise self.error
        return self.result if self.result is not None else value


@dataclass
class FakeEntryPoint:
    name: str
    value: str
    registrations: list | None = None
    error: Exception | None = None

    def load(self):
        if self.error is not None:
            raise self.error
        return lambda: self.registrations or []


def test_api_codec_001__mapping_like_values_convert_to_dict():
    assert apply_codecs(UserDict({"items": CustomSequence((1, 2))}), dag=None) == {"items": [1, 2]}


def test_api_codec_002__sequence_like_values_convert_to_list():
    assert apply_codecs(CustomSequence(("a", UserDict({"b": 2}))), dag=None) == ["a", {"b": 2}]


def test_api_codec_003__string_and_bytes_values_are_not_sequence_encoded():
    codec = MiscPyTypeCodec()

    assert not codec.can_encode("abc")
    assert not codec.can_encode(b"abc")
    assert not codec.can_encode(bytearray(b"abc"))


def test_api_codec_004__sets_are_not_list_like_values():
    assert not MiscPyTypeCodec().can_encode({1, 2})


def test_api_codec_005__builtins_include_node_and_misc_codecs():
    registrations = api.codecs()

    assert [priority for priority, _codec in registrations] == [0, 0]
    assert any(isinstance(codec, api.NodeCodec) for _priority, codec in registrations)
    assert any(isinstance(codec, api.MiscPyTypeCodec) for _priority, codec in registrations)


def test_api_codec_006__entry_points_load_once_and_sort_by_priority(monkeypatch):
    low = MatchesTypeCodec(str, "low")
    first = MatchesTypeCodec(str, "first")
    second = MatchesTypeCodec(str, "second")
    entry_points = [
        FakeEntryPoint("b", "plugin:b", [(1, low)]),
        FakeEntryPoint("a", "plugin:a", [(10, first), (10, second)]),
    ]
    monkeypatch.setattr(api, "_codecs", [])
    monkeypatch.setattr(api, "_plugins_loaded", False)
    monkeypatch.setattr(api, "_entry_points", lambda: entry_points)

    assert list(api.iter_codecs()) == [first, second, low]
    assert list(api.iter_codecs()) == [first, second, low]


def test_api_codec_007__entry_point_failure_is_codec_error(monkeypatch):
    monkeypatch.setattr(api, "_codecs", [])
    monkeypatch.setattr(api, "_plugins_loaded", False)
    monkeypatch.setattr(api, "_entry_points", lambda: [FakeEntryPoint("bad", "plugin:bad", error=RuntimeError("boom"))])

    with pytest.raises(api.CodecError, match=r"Literal codec plugin 'bad \(plugin:bad\)' failed: boom"):
        api.ensure_literal_codec_plugins_loaded()


def test_api_codec_008__apply_codec_uses_first_matching_codec(dag, monkeypatch):
    no_match = MatchesTypeCodec(int, "wrong")
    match = MatchesTypeCodec(str, 42)
    monkeypatch.setattr(api, "_codecs", [(0, 1, no_match), (0, 2, match)])
    monkeypatch.setattr(api, "_plugins_loaded", True)

    assert api.apply_codec("raw", dag=dag) == 42
    with pytest.raises(api.CodecError, match="No codec found for value of type object"):
        api.apply_codec(object(), dag=dag)


def test_api_codec_009__apply_codec_error_semantics(dag, monkeypatch):
    repo_error = DmlRepoError("repo failure")
    monkeypatch.setattr(api, "_codecs", [(0, 1, MatchesTypeCodec(str, error=repo_error))])
    monkeypatch.setattr(api, "_plugins_loaded", True)

    with pytest.raises(DmlRepoError, match="repo failure") as excinfo:
        api.apply_codec("raw", dag=dag)
    assert excinfo.value is repo_error

    monkeypatch.setattr(api, "_codecs", [(0, 1, MatchesTypeCodec(str, error=ValueError("bad")))])
    with pytest.raises(api.CodecError, match="Literal codec MatchesTypeCodec failed: bad"):
        api.apply_codec("raw", dag=dag)


def test_api_codec_010__apply_codecs_normalizes_uri_and_runnable(dag):
    runnable = Runnable(
        target=Uri(CustomSequence(("dml", "://", "target"))),
        adapter="local",
        kwargs={"items": CustomSequence((1, 2))},
        sub=UserDict({"nested": CustomSequence(("x",))}),
    )
    encoded = api.apply_codecs(runnable, dag=dag)

    assert encoded.target == Uri(["dml", "://", "target"])
    assert encoded.kwargs == {"items": [1, 2]}
    assert encoded.sub == {"nested": ["x"]}

    err = Error("boom", origin="test", type="RuntimeError")
    with pytest.raises(api.CodecError, match="No codec found for value of type Error"):
        api.apply_codecs(err, dag=dag)


def test_api_codec_011__node_codec_reuses_same_index_ref(dag, refs):
    node = api.Node(dag, refs.scalar)

    assert api.NodeCodec().encode(node, dag) == refs.scalar


def test_api_codec_012__node_codec_imports_committed_cross_dag_node(dag, fake_dml, refs):
    source = api.Dag(dml=fake_dml, ref=refs.dag2)
    node = api.Node(source, refs.scalar)

    assert api.NodeCodec().encode(node, dag) == refs.imported
    fake_dml.runtime.put_import.assert_called_once_with(refs.index, refs.dag2, node=refs.scalar, name=None)


def test_api_codec_013__node_codec_rejects_uncommitted_cross_index_node(dag, fake_dml, refs):
    source = api.Dag(dml=fake_dml, token=refs.commit)
    node = api.Node(source, refs.scalar)

    with pytest.raises(api.CodecError, match="Cannot encode node from uncommitted DAG in a different index"):
        api.NodeCodec().encode(node, dag)


def test_api_codec_014__node_codec_wraps_import_failures(dag, fake_dml, refs):
    source = api.Dag(dml=fake_dml, ref=refs.dag2)
    node = api.Node(source, refs.scalar)
    fake_dml.runtime.put_import.side_effect = RuntimeError("nope")

    with pytest.raises(api.CodecError, match="Failed to encode cross-dag node import: nope"):
        api.NodeCodec().encode(node, dag)
