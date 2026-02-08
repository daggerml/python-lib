"""GC operation CLI setup."""

from __future__ import annotations

from argparse import ArgumentParser

from daggerml._cli.base import apply_help_config, parse_ref
from daggerml._internal._db import Ref


def setup_gc_parser(parser: ArgumentParser) -> None:
    """Setup GC operation parsers and subcommands."""
    apply_help_config(
        parser,
        description="Garbage collection operations.",
        examples=[
            "dml gc run",
            "dml gc list-orphans --heads head:main head:feature",
        ],
    )
    subparsers = parser.add_subparsers(dest="method", metavar="<method>", help="Methods", required=True)

    setup_gc_run_parser(subparsers.add_parser("run", help="Run garbage collection"))
    setup_gc_list_orphans_parser(subparsers.add_parser("list-orphans", help="List orphaned objects"))


def setup_gc_run_parser(parser: ArgumentParser) -> None:
    """Setup gc run command parser."""
    apply_help_config(parser, description="Run garbage collection.", examples=["dml gc run"])
    parser.set_defaults(op="gc", method="run", func=execute_gc_run)


def setup_gc_list_orphans_parser(parser: ArgumentParser) -> None:
    """Setup gc list-orphans command parser."""
    apply_help_config(
        parser,
        description="List orphaned objects (unreachable from the provided roots).",
        examples=["dml gc list-orphans --heads head:main head:feature"],
    )
    parser.add_argument(
        "--heads",
        nargs="*",
        help="Head or index refs to use as traversal roots",
    )
    parser.set_defaults(op="gc", method="list-orphans", func=execute_gc_list_orphans)


def execute_gc_run(ops, args) -> dict[str, int]:
    """Execute gc run command."""
    return ops.gc()


def execute_gc_list_orphans(ops, args) -> list[Ref]:
    """Execute gc list-orphans command."""
    heads = parse_heads(args.heads)
    return ops.list_orphans(heads)


def parse_heads(heads: list[str] | None) -> list[Ref] | None:
    """Parse optional head strings into Ref objects."""
    if heads is None:
        return None
    parsed: list[Ref] = []
    for head in heads:
        ref = parse_ref(head)
        try:
            ref.ns()
        except ValueError as exc:
            raise ValueError(f"Invalid ref format: {head}") from exc
        parsed.append(ref)
    return parsed
