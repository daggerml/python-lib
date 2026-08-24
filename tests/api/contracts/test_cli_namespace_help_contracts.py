from __future__ import annotations

import argparse

import pytest

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
    assert "skills              Bundled agent-guidance exports." in root_help
    assert root_help.index("commands:") < root_help.index("namespaces:")


def test_cli_namespace_help_003__skills_help_lists_commands_before_namespaces() -> None:
    cli = MethodCLI(Dml, prog="dml")

    subparsers = next(action for action in cli.parser._actions if isinstance(action, argparse._SubParsersAction))
    skills_help = subparsers.choices["skills"].format_help()
    skills_subparsers = next(
        action for action in subparsers.choices["skills"]._actions if isinstance(action, argparse._SubParsersAction)
    )

    assert "commands:" in skills_help
    assert set(skills_subparsers.choices) == {
        "authoring",
        "repository",
        "inspection",
    }
    assert "namespaces:" not in skills_help


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
    cancel_help = runtime_subparsers.choices["cancel"].format_help()
    assert "--max-retries MAX_RETRIES" in cancel_help
    assert "--mode" not in cancel_help

    parsed = cli.parser.parse_args(["branch", "list", "--remote", "--dep", "models"])
    assert parsed.remote is True
    assert parsed.dep == "models"

    parsed = cli.parser.parse_args(["runtime", "cancel", "index:e1", "--max-retries", "5"])
    assert parsed.execution == Ref("index:e1")
    assert parsed.max_retries == 5


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

    assert "admin" not in root_subparsers.choices

    parsed = cli.parser.parse_args(["gc", "--remote"])
    assert parsed.remote is True


@pytest.mark.parametrize(
    "route",
    [
        ["admin", "remote", "get-cache", "cache-key"],
        ["admin", "remote", "invalidate-cache", "index:e1"],
        ["admin", "gc"],
        ["admin", "remote", "gc"],
        ["admin", "agent-skill"],
    ],
)
def test_cli_removed_maintenance_routes_are_rejected(route) -> None:
    cli = MethodCLI(Dml, prog="dml")

    with pytest.raises(SystemExit, match="2"):
        cli.parser.parse_args(route)


@pytest.mark.parametrize(
    "route",
    [
        ["cache", "get", "cache-key"],
        ["cache", "invalidate", "index:e1"],
        ["gc"],
        ["gc", "--remote"],
        ["skills", "authoring"],
        ["skills", "repository"],
        ["skills", "inspection"],
    ],
)
def test_cli_canonical_maintenance_routes_are_accepted(route) -> None:
    MethodCLI(Dml, prog="dml").parser.parse_args(route)


def test_cli_shallow_clone_fetch_and_pull_options_are_generated() -> None:
    cli = MethodCLI(Dml, prog="dml")
    root_subparsers = next(
        action for action in cli.parser._actions if isinstance(action, argparse._SubParsersAction)
    )

    clone_help = root_subparsers.choices["clone"].format_help()
    fetch_help = root_subparsers.choices["fetch"].format_help()
    pull_help = root_subparsers.choices["pull"].format_help()
    assert "--depth DEPTH" in clone_help
    assert "--unshallow" not in clone_help
    assert "--depth DEPTH" in fetch_help
    assert "--unshallow" in fetch_help
    assert "--dep DEP" in fetch_help
    assert "--depth DEPTH" in pull_help
    assert "--unshallow" not in pull_help

    parsed = cli.parser.parse_args(["fetch", "main", "--dep", "models", "--depth", "2"])
    assert parsed.revision == "main"
    assert parsed.dep == "models"
    assert parsed.depth == 2
    assert parsed.unshallow is False

    parsed = cli.parser.parse_args(["fetch", "main", "--unshallow"])
    assert parsed.unshallow is True

    parsed = cli.parser.parse_args(["pull", "--depth", "3"])
    assert parsed.depth == 3
