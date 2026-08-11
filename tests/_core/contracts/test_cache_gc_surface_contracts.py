from __future__ import annotations

import inspect
from dataclasses import replace
from typing import Any, get_args, get_type_hints

import pytest

import daggerml._core.dml as dml_mod
from daggerml._core.db import Ref
from daggerml._core.dml import Dml, LocalGCSummary, RemoteGCSummary
from daggerml._core.types import DmlRepoError
from tests._core.helpers import make_local_dml


def test_cache_get_returns_present_and_absent_refs(tmp_path, monkeypatch) -> None:
    dml = make_local_dml(tmp_path, monkeypatch)
    cached = Ref("dag:" + "a" * 64)
    calls = []

    class Remote:
        def get_cache(self, cache_key, *, raw, db):
            calls.append((cache_key, raw, db))
            return cached if cache_key == "present" else None

    monkeypatch.setattr(dml_mod, "_remote_ops", lambda _dml: Remote())

    assert dml.cache.get("present") == cached
    assert dml.cache.get("missing") is None
    assert calls == [("present", False, dml._db), ("missing", False, dml._db)]


def test_cache_invalidate_validates_before_exact_delegation(tmp_path, monkeypatch) -> None:
    dml = make_local_dml(tmp_path, monkeypatch, user="alice")
    calls = []
    response = {"total_time": 0.0, "invalidations": []}

    class State:
        def invalidate_cache(self, cache_keys, requested_by):
            calls.append((cache_keys, requested_by))
            return response

    monkeypatch.setattr(dml_mod, "_exec_state", lambda _dml, cache_key=None: State())

    assert dml.cache.invalidate("ck1", "cache-key") == response
    assert calls == [(("ck1", "cache-key"), "alice")]

    invalid_cases: list[tuple[Any, ...]] = [(), (Ref("dag:abc"),), ("dag:abc",), ("bad/key",), ("",)]
    for invalid in invalid_cases:
        with pytest.raises(ValueError):
            dml.cache.invalidate(*invalid)
    assert calls == [(("ck1", "cache-key"), "alice")]


def test_cache_surface_has_exact_runtime_metadata_and_no_admin_aliases(tmp_path, monkeypatch) -> None:
    dml = make_local_dml(tmp_path, monkeypatch)
    get_hints = get_type_hints(type(dml.cache).get, include_extras=True)
    invalidate_hints = get_type_hints(type(dml.cache).invalidate, include_extras=True)

    assert get_args(get_hints["cache_key"])[0] is str
    assert get_args(invalidate_hints["cache_keys"])[0] is str
    assert not hasattr(dml.admin, "remote")
    assert not hasattr(dml.admin, "gc")
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
