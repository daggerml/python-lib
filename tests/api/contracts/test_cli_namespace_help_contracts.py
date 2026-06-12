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
