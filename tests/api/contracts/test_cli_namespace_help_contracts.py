from __future__ import annotations

import argparse

from daggerml._cli import MethodCLI
from daggerml._core import Dml


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
    assert "admin               Administrative and remote maintenance commands." in root_help
    assert root_help.index("commands:") < root_help.index("namespaces:")


def test_cli_namespace_help_003__nested_help_lists_commands_before_namespaces() -> None:
    cli = MethodCLI(Dml, prog="dml")

    subparsers = next(action for action in cli.parser._actions if isinstance(action, argparse._SubParsersAction))
    admin_help = subparsers.choices["admin"].format_help()

    assert "commands:" in admin_help
    assert "namespaces:" in admin_help
    assert "gc         Garbage-collect unreachable local objects." in admin_help
    assert "remote     Remote cache, refs, and GC commands." in admin_help
    assert admin_help.index("commands:") < admin_help.index("namespaces:")


def test_cli_named_remote_commands_and_branch_create_options_are_generated() -> None:
    cli = MethodCLI(Dml, prog="dml")
    subparsers = next(action for action in cli.parser._actions if isinstance(action, argparse._SubParsersAction))

    remote_help = subparsers.choices["remote"].format_help()
    branch_help = subparsers.choices["branch"].format_help()
    branch_subparsers = next(
        action for action in subparsers.choices["branch"]._actions if isinstance(action, argparse._SubParsersAction)
    )
    create_help = branch_subparsers.choices["create"].format_help()

    assert "add" in remote_help and "delete" in remote_help and "list" in remote_help
    assert "set-upstream" in branch_help
    assert "name" in create_help
    assert "--remote REMOTE" in create_help
    assert "--revision REVISION" in create_help
