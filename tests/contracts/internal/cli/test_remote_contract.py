"""CLI tests for remote command wiring."""

from __future__ import annotations

import builtins
import importlib
import sys
from argparse import ArgumentParser
from io import StringIO

import pytest

from daggerml._cli import cli
from daggerml._cli.remote import require_boto3, setup_remote_parser
from daggerml._internal.types import DmlRepoError


class TestSetupRemoteParser:
    def test_parser_creation(self):
        parser = ArgumentParser()
        setup_remote_parser(parser)

        args = parser.parse_args(["push", "main"])
        assert args.method == "push"

        args = parser.parse_args(["pull", "tags/main/v1.json"])
        assert args.method == "pull"

        args = parser.parse_args(["list", "commits"])
        assert args.method == "list"

        args = parser.parse_args(["prune"])
        assert args.method == "prune"

        args = parser.parse_args(["gc"])
        assert args.method == "gc"

    def test_s3_args_not_supported(self):
        parser = ArgumentParser()
        setup_remote_parser(parser)
        with pytest.raises(SystemExit):
            parser.parse_args(["list", "commits", "--s3-bucket", "b", "--s3-prefix", "p"])


class TestRequireBoto3:
    def test_missing_boto3_raises_dm_repo_error(self, monkeypatch):
        def _boom(_name: str):
            raise ImportError("nope")

        monkeypatch.setattr(importlib, "import_module", _boom)
        with pytest.raises(DmlRepoError, match="Remote commands require boto3; install boto3 to continue"):
            require_boto3()


def test_non_remote_commands_do_not_import_boto3(monkeypatch, tmp_path):
    # Ensure boto3 isn't already present (it may be imported by other tests).
    for mod in ["boto3", "daggerml._internal.ops.remote"]:
        if mod in sys.modules:
            monkeypatch.delitem(sys.modules, mod, raising=False)

    orig_import = builtins.__import__

    def guarded_import(name, globals=None, locals=None, fromlist=(), level=0):
        if name == "boto3" or name.startswith("boto3."):
            raise ImportError("blocked boto3 import")
        return orig_import(name, globals, locals, fromlist, level)

    monkeypatch.setattr(builtins, "__import__", guarded_import)

    old_argv = sys.argv
    old_stdout = sys.stdout
    old_stderr = sys.stderr
    sys.argv = ["dml", "--repo", str(tmp_path), "cache", "list"]
    sys.stdout = StringIO()
    sys.stderr = StringIO()
    try:
        cli()
    except SystemExit:
        # CLI uses sys.exit for some errors; the important part is boto3 wasn't imported.
        pass
    finally:
        sys.argv = old_argv
        sys.stdout = old_stdout
        sys.stderr = old_stderr
