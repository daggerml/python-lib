from __future__ import annotations

from argparse import ArgumentParser

from daggerml._cli.base import apply_help_config


def setup_branch_parser(parser: ArgumentParser) -> None:
    apply_help_config(
        parser,
        description="List local or remote-tracking branches.",
        examples=["dml branch", "dml branch --remote"],
    )
    parser.add_argument("-r", "--remote", action="store_true", help="List remote-tracking branches")
    parser.set_defaults(op="branch", func=execute_branch)


def execute_branch(dml, args) -> dict[str, object]:
    return dml.branch(remote=args.remote)
