from __future__ import annotations

from argparse import ArgumentParser

from daggerml._cli.base import apply_help_config


def setup_show_parser(parser: ArgumentParser) -> None:
    apply_help_config(
        parser,
        description="Show one revision with full DAG state and commit delta.",
        examples=["dml show", "dml show HEAD~1", "dml show main"],
    )
    parser.add_argument("revision", nargs="?", default="HEAD")
    parser.set_defaults(op="show", func=execute_show)


def execute_show(dml, args) -> dict[str, object]:
    return dml.show(args.revision)
