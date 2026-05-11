"""DAG CLI setup."""

from __future__ import annotations

from argparse import ArgumentParser

from daggerml._cli.base import apply_help_config


def setup_dag_parser(parser: ArgumentParser) -> None:
    apply_help_config(
        parser,
        description="Inspect and mutate named DAGs.",
        examples=[
            "dml dag list --revision HEAD~1",
            "dml dag get train --revision main",
            "dml dag get dag:abc123",
            "dml dag checkout HEAD~1 train --as baseline_train",
            "dml dag delete train --user alice",
        ],
    )
    subparsers = parser.add_subparsers(dest="method", metavar="<method>", help="DAG methods", required=True)

    list_parser = subparsers.add_parser("list", help="List DAGs in a revision")
    apply_help_config(list_parser, description="Return the DAG map for a revision.")
    list_parser.add_argument("--revision", default="HEAD")
    list_parser.set_defaults(op="dag", method="list", func=execute_dag_list)

    get_parser = subparsers.add_parser("get", help="Get one DAG by name or ref")
    apply_help_config(get_parser, description="Load one DAG from a revision tree or by exact dag ref.")
    get_parser.add_argument("selector")
    get_parser.add_argument("--revision", default=None)
    get_parser.set_defaults(op="dag", method="get", func=execute_dag_get)

    checkout_parser = subparsers.add_parser("checkout", help="Copy a DAG from history")
    apply_help_config(checkout_parser, description="Copy one DAG from a revision into the current branch.")
    checkout_parser.add_argument("revision")
    checkout_parser.add_argument("source_name")
    checkout_parser.add_argument("--as", dest="target_name")
    checkout_parser.add_argument("--replace", action="store_true")
    checkout_parser.add_argument("--branch", default=None)
    checkout_parser.add_argument("--user", default=None)
    checkout_parser.set_defaults(op="dag", method="checkout", func=execute_dag_checkout)

    delete_parser = subparsers.add_parser("delete", help="Delete a DAG from a branch")
    apply_help_config(delete_parser, description="Delete one named DAG from a branch and commit the change.")
    delete_parser.add_argument("name")
    delete_parser.add_argument("--branch", default=None)
    delete_parser.add_argument("--user", default=None)
    delete_parser.set_defaults(op="dag", method="delete", func=execute_dag_delete)


def execute_dag_list(dml, args) -> dict[str, object]:
    return dml.dag.list(args.revision)


def execute_dag_get(dml, args) -> dict[str, object]:
    return dml.dag.get(args.selector, revision=args.revision)


def execute_dag_checkout(dml, args) -> str:
    return str(
        dml.dag.checkout(
            args.revision,
            args.source_name,
            branch=args.branch,
            target_name=args.target_name,
            replace=args.replace,
            user=args.user,
        )
    )


def execute_dag_delete(dml, args) -> str:
    return str(dml.dag.delete(args.name, branch=args.branch, user=args.user))
