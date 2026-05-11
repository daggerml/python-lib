from argparse import ArgumentParser, Namespace
from unittest.mock import Mock

import pytest

from daggerml._cli.admin import (
    execute_admin_cache_invalidate,
    execute_admin_gc,
    execute_admin_index_delete,
    execute_admin_index_get,
    execute_admin_index_list,
    execute_admin_remote_gc,
    execute_admin_remote_list,
    setup_admin_parser,
)
from daggerml._internal.types import DmlRepoError


class TestSetupAdminParser:
    def test_parser_creation(self):
        parser = ArgumentParser()
        setup_admin_parser(parser)
        assert parser.parse_args(["index", "list"]).func is execute_admin_index_list
        assert parser.parse_args(["cache", "invalidate", "ck1"]).func is execute_admin_cache_invalidate
        assert parser.parse_args(["remote", "list"]).func is execute_admin_remote_list
        assert parser.parse_args(["remote", "gc"]).func is execute_admin_remote_gc
        assert parser.parse_args(["gc", "--dry-run"]).func is execute_admin_gc


def test_execute_admin_index_handlers_delegate_to_dml():
    dml = Mock()
    dml.admin.index.list.return_value = {"indexes": []}
    dml.admin.index.get.return_value = {"index": {"id": "idx1"}}
    dml.admin.index.delete.return_value = {"index": "idx1", "deleted": True}

    assert execute_admin_index_list(dml, Namespace()) == {"indexes": []}
    assert execute_admin_index_get(dml, Namespace(index_id="idx1")) == {"index": {"id": "idx1"}}
    assert execute_admin_index_delete(dml, Namespace(index_id="idx1")) == {"index": "idx1", "deleted": True}


def test_execute_admin_cache_invalidate_delegates_to_dml():
    dml = Mock()
    dml.admin.cache.invalidate.return_value = {"cache_keys": ["ck1"], "invalidated": {"count": 1}}

    result = execute_admin_cache_invalidate(dml, Namespace(cache_keys=["ck1"]))

    dml.admin.cache.invalidate.assert_called_once_with(["ck1"])
    assert result["cache_keys"] == ["ck1"]


def test_execute_admin_remote_list_rejects_owner_with_project():
    with pytest.raises(DmlRepoError, match="--owner cannot be combined"):
        execute_admin_remote_list(Mock(), Namespace(project="dml://alice/demo", owner="alice"))


def test_execute_admin_remote_and_gc_delegate_to_dml():
    dml = Mock()
    dml.admin.remote.list.return_value = {"projects": ["dml://alice/demo"]}
    dml.admin.remote.gc.return_value = {"deleted_refs": 1}
    dml.admin.gc.return_value = {"dry_run": True, "would_delete": 0, "orphans": []}

    assert execute_admin_remote_list(dml, Namespace(project=None, owner=None)) == {"projects": ["dml://alice/demo"]}
    assert execute_admin_remote_gc(dml, Namespace()) == {"deleted_refs": 1}
    assert execute_admin_gc(dml, Namespace(dry_run=True)) == {"dry_run": True, "would_delete": 0, "orphans": []}
