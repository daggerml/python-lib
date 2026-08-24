from __future__ import annotations

import inspect
from dataclasses import replace
from typing import Any, get_args, get_type_hints

import pytest

import daggerml._core.dml as dml_mod
from daggerml._core.db import Ref
from daggerml._core.dml import CacheDescription, Dml, LocalGCSummary, RemoteGCSummary
from daggerml._core.types import DmlRepoError
from tests._core.helpers import make_local_dml


def test_cache_get_returns_present_and_absent_refs(tmp_path, monkeypatch) -> None:
    dml = make_local_dml(tmp_path, monkeypatch)
    cached = Ref("dag:" + "a" * 64)
    calls = []

    class State:
        def get_cached_result(self, cache_key, db):
            calls.append((cache_key, db))
            return cached if cache_key == "present" else None

    monkeypatch.setattr(dml_mod, "_exec_state", lambda _dml, cache_key=None: State())

    assert dml.cache.get("present") == cached
    assert dml.cache.get("missing") is None
    assert calls == [("present", dml._db), ("missing", dml._db)]


def test_cache_describe_returns_exact_ref_identities(tmp_path, monkeypatch) -> None:
    dml = make_local_dml(tmp_path, monkeypatch)
    calls = []

    class State:
        def describe_cache(self, cache_key):
            calls.append(cache_key)
            return {"execution_id": "exec", "result_ref": "dag:result", "lifecycle": "succeeded"}

    monkeypatch.setattr(dml_mod, "_exec_state", lambda _dml, cache_key=None: State())

    assert dml.cache.describe("ck1") == {
        "execution": Ref("index:exec"),
        "dag": Ref("dag:result"),
        "lifecycle": "succeeded",
    }
    assert calls == ["ck1"]


def test_cache_invalidate_validates_execution_refs_before_exact_delegation(tmp_path, monkeypatch) -> None:
    dml = make_local_dml(tmp_path, monkeypatch, user="alice")
    calls = []
    response = {"total_time": 0.0, "invalidations": []}

    class State:
        def invalidate_executions(self, execution_ids, requested_by):
            calls.append((execution_ids, requested_by))
            return response

    monkeypatch.setattr(dml_mod, "_exec_state", lambda _dml, cache_key=None: State())

    assert dml.cache.invalidate(Ref("index:e1"), Ref("frozenindex:e2")) == response
    assert calls == [(("e1", "e2"), "alice")]

    invalid_cases: list[tuple[Any, ...]] = [(), (Ref("dag:abc"),), ("index:e1",), ("e1",)]
    for invalid in invalid_cases:
        with pytest.raises((TypeError, ValueError)):
            dml.cache.invalidate(*invalid)
    assert calls == [(("e1", "e2"), "alice")]


def test_cache_surface_has_exact_runtime_metadata_and_no_admin_aliases(tmp_path, monkeypatch) -> None:
    dml = make_local_dml(tmp_path, monkeypatch)
    get_hints = get_type_hints(type(dml.cache).get, include_extras=True)
    describe_hints = get_type_hints(type(dml.cache).describe, include_extras=True)
    invalidate_hints = get_type_hints(type(dml.cache).invalidate, include_extras=True)

    assert get_args(get_hints["cache_key"])[0] is str
    assert get_args(describe_hints["cache_key"])[0] is str
    assert CacheDescription in get_args(describe_hints["return"])
    assert get_args(invalidate_hints["executions"])[0] is Ref
    assert not hasattr(dml, "admin")
    assert not hasattr(dml.cache, "get_cache")
    assert not hasattr(dml.cache, "invalidate_cache")


def test_gc_defaults_local_without_accessing_remote(tmp_path, monkeypatch) -> None:
    dml = make_local_dml(tmp_path, monkeypatch)
    dml._config = replace(dml._config, remote=replace(dml._config.remote, root=None))

    def fail_remote(_dml):
        raise AssertionError("local GC must not resolve remote state")

    monkeypatch.setattr(dml_mod, "_remote_ops", fail_remote)

    result = dml.gc()

    assert set(result) == {"deleted", "ref-enumeration-time", "gc-time"}
    assert isinstance(result["deleted"], dict)


def test_gc_remote_selection_preserves_summary_and_requires_remote_root(tmp_path, monkeypatch) -> None:
    dml = make_local_dml(tmp_path, monkeypatch)
    original_remote_ops = dml_mod._remote_ops
    summary = {
        "tombstones-deleted": 1,
        "cas-deleted": 2,
        "cas-retained": 3,
        "total-refs": 4,
        "gc-time": 5,
        "ref-enumeration-time": 6,
        "cas-enumeration-time": 7,
    }

    class Remote:
        def gc(self):
            return summary

    monkeypatch.setattr(dml_mod, "_remote_ops", lambda _dml: Remote())
    assert dml.gc(remote=True) == summary

    dml._config = replace(dml._config, remote=replace(dml._config.remote, root=None))
    monkeypatch.setattr(dml_mod, "_remote_ops", original_remote_ops)
    with pytest.raises(DmlRepoError, match="remote.root is required"):
        dml.gc(remote=True)


def test_gc_signature_exposes_only_remote_and_union_summary() -> None:
    signature = inspect.signature(Dml.gc)
    hints = get_type_hints(Dml.gc, include_extras=True)

    assert list(signature.parameters) == ["self", "remote"]
    assert signature.parameters["remote"].kind is inspect.Parameter.KEYWORD_ONLY
    assert signature.parameters["remote"].default is False
    assert get_args(hints["remote"])[0] is bool
    assert set(get_args(hints["return"])) == {LocalGCSummary, RemoteGCSummary}
