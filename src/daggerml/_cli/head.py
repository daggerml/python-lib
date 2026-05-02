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
            "dml head create feature --from main",
            "dml head delete feature",
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
        examples=["dml head create feature --from main"],
    )
    create_parser.add_argument("branch_name", help="Branch name")
    create_parser.add_argument("--from", dest="from_head", help="Source branch name or commit ref")
    create_parser.set_defaults(func=execute_head_create)

    # delete subcommand
    delete_parser = subparsers.add_parser("delete", help="Delete head")
    apply_help_config(
        delete_parser,
        description="Delete a branch.",
        examples=["dml head delete feature"],
    )
    delete_parser.add_argument("branch_name", help="Branch name")
    delete_parser.set_defaults(func=execute_head_delete)


def execute_head_list(head_ops, args) -> List[str]:
    """Execute head list command, return JSON-serializable result."""
    return head_ops.list_branches()


def execute_head_create(head_ops, args) -> Any:
    """Execute head create command, return JSON-serializable result."""
    from_commit = None
    if args.from_head:
        parsed_from = parse_ref(args.from_head) if ":" in args.from_head else parse_ref(f"head:{args.from_head}")
        from_commit = parsed_from if parsed_from.ns() == "commit" else head_ops.get_branch_commit(parsed_from.id())
    result = head_ops.create_branch(args.branch_name, from_commit)
    return {
        "branch": result,
    }


def execute_head_delete(head_ops, args) -> None:
    """Execute head delete command, return JSON-serializable result."""
    head_ops.delete_branch(args.branch_name)
    return None
