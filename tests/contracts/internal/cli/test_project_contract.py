from argparse import Namespace
from unittest.mock import Mock

import daggerml._cli.project as project_cli
from daggerml._cli.project import ProjectAliasHandlers


def test_fetch_alias_delegates_to_dml():
    ops = Mock()
    args = Namespace(remote_or_uri="origin", branch=None)
    ops.fetch.return_value = "commit:1"

    result = ProjectAliasHandlers.fetch(ops, args)

    ops.fetch.assert_called_once_with("origin", None)
    assert result == "commit:1"


def test_pull_alias_delegates_to_dml():
    ops = Mock()
    args = Namespace(remote_or_uri="origin", branch=None, branch_name=None, user="alice")
    ops.pull.return_value = "commit:1"

    result = ProjectAliasHandlers.pull(ops, args)

    ops.pull.assert_called_once_with("origin", None, branch=None, user="alice")
    assert result == "commit:1"


def test_push_alias_delegates_to_dml():
    ops = Mock()
    args = Namespace(tag=None, branch_name=None, create=False, force=False)
    ops.push.return_value = "commit:1"

    result = ProjectAliasHandlers.push(ops, args)

    ops.push.assert_called_once_with(None, branch=None, create=False, force=False)
    assert result == "commit:1"


def test_push_alias_with_tag_delegates_to_dml():
    ops = Mock()
    args = Namespace(tag="v1.0", branch_name=None, create=False, force=False)
    ops.push.return_value = "projects/alice/demo/tags/v1.0.json"

    result = ProjectAliasHandlers.push(ops, args)

    ops.push.assert_called_once_with("v1.0", branch=None, create=False, force=False)
    assert result == "projects/alice/demo/tags/v1.0.json"


def test_checkout_alias_delegates_to_dml():
    ops = Mock()
    args = Namespace(revision="main")
    ops.checkout.return_value = {"mode": "attached", "branch": "main"}

    result = ProjectAliasHandlers.checkout(ops, args)

    ops.checkout.assert_called_once_with("main")
    assert result["mode"] == "attached"


def test_merge_alias_delegates_to_dml():
    ops = Mock()
    args = Namespace(revision="origin/main", branch_name=None, user="alice")
    ops.merge.return_value = "commit:2"

    result = ProjectAliasHandlers.merge(ops, args)

    ops.merge.assert_called_once_with("origin/main", None, "alice")
    assert result == "commit:2"


def test_revert_alias_delegates_to_dml():
    ops = Mock()
    args = Namespace(revision="origin/main", branch_name=None, user="alice")
    ops.revert.return_value = "commit:2"

    result = ProjectAliasHandlers.revert(ops, args)

    ops.revert.assert_called_once_with("origin/main", None, "alice")
    assert result == "commit:2"


def test_project_alias_cli_module_avoids_remote_client_wiring_imports():
    assert not hasattr(project_cli, "require_boto3")
    assert not hasattr(project_cli, "create_s3_client")
