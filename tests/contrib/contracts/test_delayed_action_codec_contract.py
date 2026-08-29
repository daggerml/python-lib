from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock

from daggerml import Runnable, Uri
from daggerml.contrib.codecs import DelayedActionCodec, DelayedLoad, DelayedRef, DelayedRunnable


def test_contrib_codec_001__delayed_ref_resolves_named_node(monkeypatch):
    codec = DelayedActionCodec()
    dag = MagicMock()
    node = object()
    dag.__getitem__.return_value = node
    encoded = object()
    seen = []

    def fake_apply_codecs(value, *, dag):
        seen.append((value, dag))
        return encoded

    monkeypatch.setattr("daggerml.contrib.codecs.apply_codecs", fake_apply_codecs)

    delayed = DelayedRef(name="answer")
    assert codec.can_encode(delayed)
    assert codec.encode(delayed, dag) is encoded
    dag.__getitem__.assert_called_once_with("answer")
    assert seen == [(node, dag)]


def test_contrib_codec_002__delayed_load_imports_requested_node(monkeypatch):
    codec = DelayedActionCodec()
    dag = MagicMock()
    node = object()
    dag.require.return_value = node
    encoded = object()
    seen = []

    def fake_apply_codecs(value, *, dag):
        seen.append((value, dag))
        return encoded

    monkeypatch.setattr("daggerml.contrib.codecs.apply_codecs", fake_apply_codecs)

    delayed = DelayedLoad(dagname="source-dag", nodename="result")
    assert codec.can_encode(delayed)
    assert codec.encode(delayed, dag) is encoded
    dag.require.assert_called_once_with("source-dag", name="result")
    assert seen == [(node, dag)]


def test_contrib_codec_003__delayed_runnable_resolves_through_adapter(monkeypatch):
    codec = DelayedActionCodec()
    adapter = SimpleNamespace()
    delayed = DelayedRunnable(
        uri="script",
        adapter="local",
        sub=DelayedRunnable(uri="script", adapter="local", sub=None, kwargs={"fn": "nested"}),
        kwargs={"image": Uri("s3://bucket/image.tar")},
    )
    runnable = Runnable(target=Uri("script"), adapter="local", kwargs={"image": Uri("s3://bucket/image.tar")})
    adapter.resolve_runnable = MagicMock(return_value=runnable)

    monkeypatch.setattr("daggerml.contrib.codecs.get_adapter", lambda name: adapter)

    assert codec.can_encode(delayed)
    assert codec.encode(delayed, MagicMock()) is runnable
    adapter.resolve_runnable.assert_called_once_with("script", sub=delayed.sub, kwargs=delayed.kwargs)
