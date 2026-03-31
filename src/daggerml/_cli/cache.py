"""Cache operation CLI setup."""

import argparse
from argparse import ArgumentParser
from typing import Optional

from daggerml._cli.base import apply_help_config, parse_ref


def setup_cache_parser(parser: ArgumentParser) -> None:
    """Setup cache operation parsers and subcommands."""
    apply_help_config(
        parser,
        description="Cache operations: store and retrieve cached DAG results keyed by argv node.",
        examples=[
            "dml cache put dag:<id>",
            "dml cache get node-argv:<id>",
            "dml cache delete node-argv:<id>",
            "dml cache list --limit 10",
            "dml cache clear",
        ],
    )
    subparsers = parser.add_subparsers(dest="subcommand", metavar="<method>", help="Methods")

    setup_cache_put_parser(subparsers.add_parser("put", help="Store cache entry for a DAG"))
    setup_cache_get_parser(subparsers.add_parser("get", help="Get cached DAG for argv"))
    setup_cache_delete_parser(subparsers.add_parser("delete", help="Delete cache entry for argv"))
    setup_cache_list_parser(subparsers.add_parser("list", help="List cache entries"))
    setup_cache_clear_parser(subparsers.add_parser("clear", help="Clear all cache entries"))


def setup_cache_put_parser(put_parser: ArgumentParser) -> None:
    """Setup cache put subcommand parser."""
    apply_help_config(
        put_parser,
        description="Store a cache entry for a DAG.",
        examples=["dml cache put dag:abc123"],
    )
    put_parser.add_argument("dag_ref", help="DAG ref (dag:<id>)")
    put_parser.set_defaults(func=execute_cache_put)


def setup_cache_get_parser(get_parser: ArgumentParser) -> None:
    """Setup cache get subcommand parser."""
    apply_help_config(
        get_parser,
        description="Get the cached DAG ref for an argv node.",
        examples=["dml cache get node-argv:def456"],
    )
    get_parser.add_argument("argv_ref", help="Argv ref (node-argv:<id>)")
    get_parser.set_defaults(func=execute_cache_get)


def setup_cache_delete_parser(delete_parser: ArgumentParser) -> None:
    """Setup cache delete subcommand parser."""
    apply_help_config(
        delete_parser,
        description="Delete a cache entry by argv node.",
        examples=["dml cache delete node-argv:def456"],
    )
    delete_parser.add_argument("argv_ref", help="Argv ref (node-argv:<id>)")
    delete_parser.set_defaults(func=execute_cache_delete)


def setup_cache_list_parser(list_parser: ArgumentParser) -> None:
    """Setup cache list subcommand parser."""
    apply_help_config(
        list_parser,
        description="List cache entries.",
        examples=["dml cache list --limit 10"],
    )
    list_parser.add_argument("--limit", type=_positive_int, help="Maximum number of entries to return")
    list_parser.set_defaults(func=execute_cache_list)


def setup_cache_clear_parser(clear_parser: ArgumentParser) -> None:
    """Setup cache clear subcommand parser."""
    apply_help_config(
        clear_parser,
        description="Clear all cache entries.",
        examples=["dml cache clear"],
    )
    clear_parser.set_defaults(func=execute_cache_clear)


def execute_cache_put(ops_obj, args):
    """Execute cache put command, return JSON-serializable result."""
    dag_ref = parse_ref(args.dag_ref)
    return ops_obj.put(dag_ref)


def execute_cache_get(ops_obj, args) -> Optional[object]:
    """Execute cache get command, return JSON-serializable result."""
    argv_ref = parse_ref(args.argv_ref)
    result = ops_obj.get(argv_ref)
    return result.to if result is not None else None


def execute_cache_delete(ops_obj, args) -> bool:
    """Execute cache delete command, return JSON-serializable result."""
    argv_ref = parse_ref(args.argv_ref)
    result = ops_obj.delete(argv_ref)
    return result


def execute_cache_list(ops_obj, args):
    """Execute cache list command, return JSON-serializable result."""
    result = ops_obj.list(args.limit)
    return [[cache_key, {"dag": dag_ref.to}] for cache_key, dag_ref in result]


def execute_cache_clear(ops_obj, args) -> int:
    """Execute cache clear command, return JSON-serializable result."""
    result = ops_obj.clear()
    return result


def _positive_int(value: str) -> int:
    """Validate that a CLI argument is a positive integer."""
    try:
        parsed = int(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("limit must be a positive integer") from exc
    if parsed <= 0:
        raise argparse.ArgumentTypeError("limit must be a positive integer")
    return parsed
