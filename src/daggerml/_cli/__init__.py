from argparse import ArgumentParser

from . import base
from .admin import setup_admin_parser
from .branch import setup_branch_parser
from .config import setup_config_parser
from .dag import setup_dag_parser
from .diff import setup_diff_parser
from .init import setup_init_parser
from .log import setup_log_parser
from .project import setup_project_alias_parser
from .show import setup_show_parser
from .status import setup_status_parser


def cli() -> None:
    """Main CLI entry point."""
    parser = ArgumentParser(prog="dml")
    base.apply_help_config(
        parser,
        description=(
            "DaggerML CLI\n\n"
            "Command groups:\n"
            "  Bootstrap: init\n"
            "  Porcelain: status, show, log, diff, checkout, branch, fetch, pull, push, merge, revert\n"
            "  Namespaces: dag, admin, config"
        ),
        examples=[
            "dml --help",
            "dml --project-home /path/to/repo status",
            "dml log main --limit 10",
            "dml admin index list",
        ],
    )
    parser.add_argument(
        "--project-home",
        dest="project_home",
        type=str,
        help="Project home path (defaults to $DML_PROJECT_HOME)",
    )
    parser.add_argument(
        "--remote-uri",
        dest="runtime_remote_uri",
        type=str,
        help="Remote project URI (defaults to $DML_REMOTE_URI)",
    )
    parser.add_argument("-v", "--verbose", action="count", default=0, help="Increase verbosity")
    subparsers = parser.add_subparsers(dest="op", metavar="<operation>", help="Operations")

    # Init subcommand
    init_parser = subparsers.add_parser("init", help="Initialize .dml state in current or --project-home directory")
    setup_init_parser(init_parser)

    # Status subcommand
    status_parser = subparsers.add_parser("status", help="Show repository and runtime status")
    setup_status_parser(status_parser)

    show_parser = subparsers.add_parser("show", help="Show one revision")
    setup_show_parser(show_parser)

    log_parser = subparsers.add_parser("log", help="List commit history")
    setup_log_parser(log_parser)

    diff_parser = subparsers.add_parser("diff", help="Compare two revisions")
    setup_diff_parser(diff_parser)

    branch_parser = subparsers.add_parser("branch", help="List local or remote-tracking branches")
    setup_branch_parser(branch_parser)

    # Config subcommand
    config_parser = subparsers.add_parser("config", help="Inspect or update configuration")
    setup_config_parser(config_parser)

    for alias in ("fetch", "pull", "push", "merge", "revert", "checkout"):
        setup_project_alias_parser(subparsers.add_parser(alias, help=f"Git-like {alias}"), alias)

    # DAG subcommand
    dag_parser = subparsers.add_parser("dag", help="Inspect or mutate DAGs")
    setup_dag_parser(dag_parser)

    admin_parser = subparsers.add_parser("admin", help="Administrative and maintenance flows")
    setup_admin_parser(admin_parser)

    args = parser.parse_args()

    base.setup_logging(args.verbose)

    if args.op:
        base.execute_command(args)
    else:
        parser.print_help()
