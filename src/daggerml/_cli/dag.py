"""DAG operation CLI setup."""

from __future__ import annotations

from argparse import ArgumentParser
from typing import Any

from daggerml._cli.base import apply_help_config, parse_ref


def setup_dag_parser(parser: ArgumentParser) -> None:
    """Setup DAG operation parsers and subcommands."""
    apply_help_config(
        parser,
        description="DAG operations: list DAGs and inspect their nodes/arguments.",
        examples=[
            "dml dag list",
            "dml dag describe dag:<id>",
            "dml dag get-node dag:<id> result",
        ],
    )
    subparsers = parser.add_subparsers(dest="method", metavar="<method>", help="Methods", required=True)

    setup_dag_list_parser(subparsers.add_parser("list", help="List DAGs"))
    setup_dag_describe_parser(subparsers.add_parser("describe", help="Describe a DAG"))
    setup_dag_get_node_parser(subparsers.add_parser("get-node", help="Get a DAG node ref"))
    setup_dag_get_argv_parser(subparsers.add_parser("get-argv", help="Get DAG argv node"))
    setup_dag_get_kwargv_parser(subparsers.add_parser("get-kwargv", help="Get DAG kwargv node"))


def setup_dag_list_parser(parser: ArgumentParser) -> None:
    """Setup dag list command parser."""
    apply_help_config(parser, description="List DAGs.", examples=["dml dag list"])
    parser.set_defaults(op="dag", method="list", func=execute_dag_list)


def setup_dag_describe_parser(parser: ArgumentParser) -> None:
    """Setup dag describe command parser."""
    apply_help_config(parser, description="Describe a DAG by ref.", examples=["dml dag describe dag:abc123"])
    parser.add_argument("dag_ref", help="DAG ref (dag:<id>)")
    parser.set_defaults(op="dag", method="describe", func=execute_dag_describe)


def setup_dag_get_node_parser(parser: ArgumentParser) -> None:
    """Setup dag get-node command parser."""
    apply_help_config(
        parser,
        description="Get a node ref by name from a DAG.",
        examples=["dml dag get-node dag:abc123 result"],
    )
    parser.add_argument("dag_ref", help="DAG ref (dag:<id>)")
    parser.add_argument("name", help="Node name (string)")
    parser.set_defaults(op="dag", method="get-node", func=execute_dag_get_node)


def setup_dag_get_argv_parser(parser: ArgumentParser) -> None:
    """Setup dag get-argv command parser."""
    apply_help_config(parser, description="Get argv node ref for a DAG.", examples=["dml dag get-argv dag:abc123"])
    parser.add_argument("dag_ref", help="DAG ref (dag:<id>)")
    parser.set_defaults(op="dag", method="get-argv", func=execute_dag_get_argv)


def execute_dag_list(ops_obj: Any, args) -> list[dict[str, Any]]:
    """Execute dag list command."""
    return ops_obj.list()


def execute_dag_describe(ops_obj: Any, args) -> dict[str, Any]:
    """Execute dag describe command."""
    dag_ref = parse_ref(args.dag_ref)
    return ops_obj.describe(dag_ref)


def execute_dag_get_node(ops_obj: Any, args) -> str:
    """Execute dag get-node command."""
    dag_ref = parse_ref(args.dag_ref)
    result = ops_obj.get_node(dag_ref, args.name)
    return result.to


def execute_dag_get_argv(ops_obj: Any, args) -> str:
    """Execute dag get-argv command."""
    dag_ref = parse_ref(args.dag_ref)
    result = ops_obj.get_argv(dag_ref)
    return result.to


def execute_dag_get_kwargv(ops_obj: Any, args) -> str:
    """Execute dag get-kwargv command."""
    dag_ref = parse_ref(args.dag_ref)
    result = ops_obj.get_kwargv(dag_ref)
    return result.to
def setup_dag_get_kwargv_parser(parser: ArgumentParser) -> None:
    """Setup dag get-kwargv command parser."""
    apply_help_config(parser, description="Get kwargv node ref for a DAG.", examples=["dml dag get-kwargv dag:abc123"])
    parser.add_argument("dag_ref", help="DAG ref (dag:<id>)")
    parser.set_defaults(op="dag", method="get-kwargv", func=execute_dag_get_kwargv)

