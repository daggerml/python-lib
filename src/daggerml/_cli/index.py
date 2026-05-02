"""Index operation CLI setup."""

from __future__ import annotations

from argparse import ArgumentParser

from daggerml._cli.base import apply_help_config


def setup_index_parser(parser: ArgumentParser) -> None:
    """Setup index operation parsers and subcommands."""
    apply_help_config(
        parser,
        description="Index operations: list, describe, and delete.",
        examples=[
            "dml index list",
            "dml index describe abc",
            "dml index delete abc",
        ],
    )
    subparsers = parser.add_subparsers(dest="method", metavar="<method>", help="Methods", required=True)

    setup_index_list_parser(subparsers.add_parser("list", help="List indexes"))
    setup_index_describe_parser(subparsers.add_parser("describe", help="Describe an index"))
    setup_index_delete_parser(subparsers.add_parser("delete", help="Delete an index"))


def setup_index_list_parser(parser: ArgumentParser) -> None:
    """Setup index list command parser."""
    apply_help_config(parser, description="List indexes.", examples=["dml index list"])
    parser.set_defaults(op="index", method="list", func=execute_index_list)


def setup_index_describe_parser(parser: ArgumentParser) -> None:
    """Setup index describe command parser."""
    apply_help_config(parser, description="Describe an index.", examples=["dml index describe abc123"])
    parser.add_argument("index_id", help="Index id")
    parser.set_defaults(op="index", method="describe", func=execute_index_describe)


def setup_index_delete_parser(parser: ArgumentParser) -> None:
    """Setup index delete command parser."""
    apply_help_config(parser, description="Delete an index.", examples=["dml index delete abc123"])
    parser.add_argument("index_id", help="Index id")
    parser.set_defaults(op="index", method="delete", func=execute_index_delete)


def execute_index_list(ops, args) -> list[str]:
    """Execute index list command."""
    return ops.list_indexes()


def execute_index_describe(ops, args) -> dict:
    """Execute index describe command."""
    return ops.describe(args.index_id)


def execute_index_delete(ops, args) -> None:
    """Execute index delete command."""
    ops.delete(args.index_id)
    return None
