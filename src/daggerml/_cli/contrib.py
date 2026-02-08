from __future__ import annotations

from daggerml._cli import base
from daggerml.contrib import status as contrib_status


def execute_contrib_status(args):
    return contrib_status.status()


def setup_contrib_parser(parser) -> None:
    base.apply_help_config(
        parser,
        description="Inspect contrib adapters, executors, and codecs",
        examples=[
            "dml contrib status",
        ],
    )
    subparsers = parser.add_subparsers(dest="subcommand", metavar="<command>", help="Contrib commands")

    status_parser = subparsers.add_parser("status", help="Show contrib plugin status")
    base.apply_help_config(
        status_parser,
        description="Show contrib plugin discovery and effective registrations",
        examples=["dml contrib status"],
    )
    status_parser.set_defaults(func=execute_contrib_status)
