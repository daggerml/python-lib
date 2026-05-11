from __future__ import annotations

from argparse import ArgumentParser

from daggerml._cli.base import apply_help_config


def setup_log_parser(parser: ArgumentParser) -> None:
    apply_help_config(
        parser,
        description="List commit entries starting from a revision.",
        examples=["dml log", "dml log main --limit 10"],
    )
    parser.add_argument("revision", nargs="?", default="HEAD")
    parser.add_argument("--limit", type=int, default=None)
    parser.set_defaults(op="log", func=execute_log)


def execute_log(dml, args) -> dict[str, object]:
    return dml.log(args.revision, limit=args.limit)
