from __future__ import annotations

from argparse import ArgumentParser

from daggerml._cli.base import apply_help_config


def setup_diff_parser(parser: ArgumentParser) -> None:
    apply_help_config(
        parser,
        description="Compare DAG maps between two revisions.",
        examples=["dml diff", "dml diff main feature"],
    )
    parser.add_argument("left", nargs="?", default="HEAD~1")
    parser.add_argument("right", nargs="?", default="HEAD")
    parser.set_defaults(op="diff", func=execute_diff)


def execute_diff(dml, args) -> dict[str, object]:
    return dml.diff(args.left, args.right)
