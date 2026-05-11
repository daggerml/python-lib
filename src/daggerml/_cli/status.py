from __future__ import annotations

from argparse import ArgumentParser

from daggerml._cli.base import apply_help_config


def execute_status(dml, _args) -> dict[str, object]:
    return dml.status()


def setup_status_parser(parser: ArgumentParser) -> None:
    apply_help_config(
        parser,
        description="Show repository and runtime status.",
        examples=[
            "dml status",
            "dml --project-home /path/to/repo status",
        ],
    )
    parser.set_defaults(op="status", func=execute_status)
