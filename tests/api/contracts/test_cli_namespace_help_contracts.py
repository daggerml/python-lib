from __future__ import annotations

import argparse

from daggerml._cli import MethodCLI
from daggerml._core import Dml, Ref


def test_cli_namespace_help_001__uses_property_annotation_and_docstring() -> None:
    cli = MethodCLI(Dml, prog="dml")

    root_help = cli.parser.format_help()

    subparsers = next(action for action in cli.parser._actions if isinstance(action, argparse._SubParsersAction))
    dag_help = subparsers.choices["dag"].format_help()

    assert "Committed DAG inspection commands." in root_help
    assert "Expose committed DAG inspection commands." in dag_help


def test_cli_namespace_help_002__root_help_lists_commands_before_namespaces() -> None:
    cli = MethodCLI(Dml, prog="dml")

    root_help = cli.parser.format_help()

    assert "commands:" in root_help
    assert "namespaces:" in root_help
    assert "checkout            Check out a different revision." in root_help
    assert "admin               Administrative support commands." in root_help
    assert root_help.index("commands:") < root_help.index("namespaces:")


def test_cli_namespace_help_003__nested_help_lists_commands_before_namespaces() -> None:
    cli = MethodCLI(Dml, prog="dml")

    subparsers = next(action for action in cli.parser._actions if isinstance(action, argparse._SubParsersAction))
    admin_help = subparsers.choices["admin"].format_help()

    assert "commands:" in admin_help
    assert "agent-skill" in admin_help
    assert "namespaces:" not in admin_help
    assert "gc" not in admin_help
    assert "remote" not in admin_help


def test_cli_dependency_commands_and_branch_create_options_are_generated() -> None:
    cli = MethodCLI(Dml, prog="dml")
    subparsers = next(action for action in cli.parser._actions if isinstance(action, argparse._SubParsersAction))

    dependency_help = subparsers.choices["dep"].format_help()
    branch_help = subparsers.choices["branch"].format_help()
    branch_subparsers = next(
        action for action in subparsers.choices["branch"]._actions if isinstance(action, argparse._SubParsersAction)
    )
    create_help = branch_subparsers.choices["create"].format_help()

    assert "add" in dependency_help and "delete" in dependency_help and "list" in dependency_help
    assert "set-upstream" in branch_help
    assert "name" in create_help
    assert "--remote" in create_help
    assert "--revision REVISION" in create_help


def test_cli_ref_listing_and_inspection_commands_are_generated() -> None:
    cli = MethodCLI(Dml, prog="dml")
    root_subparsers = next(
        action for action in cli.parser._actions if isinstance(action, argparse._SubParsersAction)
    )
    branch = root_subparsers.choices["branch"]
    tag = root_subparsers.choices["tag"]
    runtime = root_subparsers.choices["runtime"]
    branch_subparsers = next(
        action for action in branch._actions if isinstance(action, argparse._SubParsersAction)
    )
    tag_subparsers = next(action for action in tag._actions if isinstance(action, argparse._SubParsersAction))
    runtime_subparsers = next(
        action for action in runtime._actions if isinstance(action, argparse._SubParsersAction)
    )

    branch_list_help = branch_subparsers.choices["list"].format_help()
    tag_list_help = tag_subparsers.choices["list"].format_help()
    assert "--remote" in branch_list_help and "--dep DEP" in branch_list_help
    assert "--remote" in tag_list_help and "--dep DEP" in tag_list_help
    assert "get-upstream" in branch_subparsers.choices
    assert "get-upstream" not in tag_subparsers.choices
    assert "read-launch-state" not in runtime_subparsers.choices

    parsed = cli.parser.parse_args(["branch", "list", "--remote", "--dep", "models"])
    assert parsed.remote is True
    assert parsed.dep == "models"


def test_cli_cache_and_gc_surfaces_are_generated_without_admin_aliases() -> None:
    cli = MethodCLI(Dml, prog="dml")
    root_subparsers = next(
        action for action in cli.parser._actions if isinstance(action, argparse._SubParsersAction)
    )

    cache = root_subparsers.choices["cache"]
    cache_subparsers = next(
        action for action in cache._actions if isinstance(action, argparse._SubParsersAction)
    )
    assert set(cache_subparsers.choices) == {"get", "describe", "invalidate"}

    parsed = cli.parser.parse_args(["cache", "invalidate", "index:e1", "frozenindex:e2"])
    assert parsed.executions == [Ref("index:e1"), Ref("frozenindex:e2")]

    gc_help = root_subparsers.choices["gc"].format_help()
    assert "--remote" in gc_help
    assert "--dep" not in gc_help
    assert "--dry-run" not in gc_help

    admin = root_subparsers.choices["admin"]
    admin_subparsers = next(
        action for action in admin._actions if isinstance(action, argparse._SubParsersAction)
    )
    assert "remote" not in admin_subparsers.choices
    assert "gc" not in admin_subparsers.choices

    parsed = cli.parser.parse_args(["gc", "--remote"])
    assert parsed.remote is True
