"""Head operation CLI setup."""

from argparse import ArgumentParser
from typing import Any, List

from daggerml._cli.base import apply_help_config, parse_ref


def setup_head_parser(parser: ArgumentParser) -> None:
    """Setup head operation parsers and subcommands."""
    apply_help_config(
        parser,
        description="Head operations: list, create, and delete heads (branch pointers).",
        examples=[
            "dml head list",
            "dml head create feature --from head:main",
            "dml head delete head:feature",
        ],
    )
    subparsers = parser.add_subparsers(dest="subcommand", metavar="<method>", help="Methods")

    # list subcommand
    list_parser = subparsers.add_parser("list", help="List heads")
    apply_help_config(list_parser, description="List all heads in the repository.", examples=["dml head list"])
    list_parser.set_defaults(func=execute_head_list)

    # create subcommand
    create_parser = subparsers.add_parser("create", help="Create head")
    apply_help_config(
        create_parser,
        description="Create a new head, optionally from an existing head or commit.",
        examples=["dml head create feature --from head:main"],
    )
    create_parser.add_argument("branch_name", help="Head name (string; stored as head:<name>)")
    create_parser.add_argument("--from", dest="from_head", help="Source ref (head:<name> or commit:<id>)")
    create_parser.set_defaults(func=execute_head_create)

    # delete subcommand
    delete_parser = subparsers.add_parser("delete", help="Delete head")
    apply_help_config(
        delete_parser,
        description="Delete a head ref.",
        examples=["dml head delete head:feature"],
    )
    delete_parser.add_argument("head_ref", help="Head ref (head:<name>)")
    delete_parser.set_defaults(func=execute_head_delete)


def execute_head_list(head_ops, args) -> List[str]:
    """Execute head list command, return JSON-serializable result."""
    result = head_ops.list()
    return [str(ref) for ref in result]


def execute_head_create(head_ops, args) -> Any:
    """Execute head create command, return JSON-serializable result."""
    if args.from_head and ":" not in args.from_head:
        args.from_head = f"head:{args.from_head}"
    parsed_from = parse_ref(args.from_head) if args.from_head else None
    result = head_ops.create(args.branch_name, parsed_from)
    return {
        "head": result.to,
    }


def execute_head_delete(head_ops, args) -> None:
    """Execute head delete command, return JSON-serializable result."""
    parsed_head = parse_ref(args.head_ref)
    if not parsed_head.to.startswith("head:"):
        raise ValueError("Head reference must start with 'head:'")
    head_ops.delete(parsed_head)
    return None
