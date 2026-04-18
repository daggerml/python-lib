from argparse import ArgumentParser

from . import base
from .cache import setup_cache_parser
from .commit import setup_commit_parser
from .contrib import setup_contrib_parser
from .dag import setup_dag_parser
from .gc import setup_gc_parser
from .head import setup_head_parser
from .index import setup_index_parser
from .init import setup_init_parser
from .node import setup_node_parser
from .remote import setup_remote_parser


def cli() -> None:
    """Main CLI entry point."""
    parser = ArgumentParser(prog="dml")
    base.apply_help_config(
        parser,
        description="DaggerML CLI",
        examples=[
            "dml --help",
            "dml --repo /path/to/repo head list",
            "dml commit list HEAD --limit 10",
        ],
    )
    parser.add_argument("--repo", type=str, help="Repository path (defaults to $DML_REPO or cwd)")
    parser.add_argument(
        "--remote-root",
        type=str,
        help="Remote project root URI (defaults to $DML_REMOTE_ROOT)",
    )
    parser.add_argument("-v", "--verbose", action="count", default=0, help="Increase verbosity")
    subparsers = parser.add_subparsers(dest="op", metavar="<operation>", help="Operations")

    # Init subcommand
    init_parser = subparsers.add_parser("init", help="Create a named repository in config dir")
    setup_init_parser(init_parser)

    # Commit subcommand
    commit_parser = subparsers.add_parser("commit", help="Work with commits")
    setup_commit_parser(commit_parser)

    # Head subcommand
    head_parser = subparsers.add_parser("head", help="Manage heads (branches)")
    setup_head_parser(head_parser)

    # Index subcommand
    index_parser = subparsers.add_parser("index", help="Work with indexes")
    setup_index_parser(index_parser)

    # Cache subcommand
    cache_parser = subparsers.add_parser("cache", help="Manage cache entries")
    setup_cache_parser(cache_parser)

    # DAG subcommand
    dag_parser = subparsers.add_parser("dag", help="Inspect DAGs")
    setup_dag_parser(dag_parser)

    # Node subcommand
    node_parser = subparsers.add_parser("node", help="Read node values")
    setup_node_parser(node_parser)

    # Remote subcommand
    remote_parser = subparsers.add_parser("remote", help="Sync with remote (S3)")
    setup_remote_parser(remote_parser)

    # GC subcommand
    gc_parser = subparsers.add_parser("gc", help="Garbage collection")
    setup_gc_parser(gc_parser)

    # Contrib subcommand
    contrib_parser = subparsers.add_parser("contrib", help="Inspect contrib plugins")
    setup_contrib_parser(contrib_parser)

    args = parser.parse_args()

    base.setup_logging(args.verbose)

    if args.op:
        base.execute_command(args)
    else:
        parser.print_help()
