"""Node operation CLI setup."""

from argparse import ArgumentParser
from typing import Any

from daggerml._cli.base import apply_help_config, parse_ref


def setup_node_parser(node_parser: ArgumentParser) -> None:
    """Setup node operation - only knows about node methods"""

    apply_help_config(
        node_parser,
        description="Node operations: read node values and unroll node structures.",
        examples=[
            "dml node get node:<id>",
            "dml node unroll node:<id>",
        ],
    )

    method_parsers = node_parser.add_subparsers(dest="method", metavar="<method>", required=True)

    setup_node_get_parser(method_parsers.add_parser("get", help="Get node value"))
    setup_node_unroll_parser(method_parsers.add_parser("unroll", help="Unroll node completely"))


def setup_node_get_parser(get_parser: ArgumentParser) -> None:
    """Setup node get method - accepts node Ref argument"""

    apply_help_config(
        get_parser,
        description="Get a node value.",
        examples=["dml node get node:abc123"],
    )
    get_parser.add_argument("node", help="Node ref (node:<id>)")
    get_parser.set_defaults(func=execute_node_get)


def setup_node_unroll_parser(unroll_parser: ArgumentParser) -> None:
    """Setup node unroll method - accepts node Ref argument"""

    apply_help_config(
        unroll_parser,
        description="Unroll a node recursively into JSON-serializable data.",
        examples=["dml node unroll node:abc123"],
    )
    unroll_parser.add_argument("node", help="Node ref (node:<id>)")
    unroll_parser.set_defaults(func=execute_node_unroll)


def execute_node_get(ops_obj: Any, args) -> Any:
    """Execute node get operation"""

    node_ref = parse_ref(args.node)  # Convert string to Ref object
    result = ops_obj.get(node_ref)  # Call NodeOps.get()
    return result  # Return serializable result


def execute_node_unroll(ops_obj: Any, args) -> Any:
    """Execute node unroll operation"""

    node_ref = parse_ref(args.node)  # Convert string to Ref object
    result = ops_obj.unroll(node_ref)  # Call NodeOps.unroll()
    return result  # Return serializable result
