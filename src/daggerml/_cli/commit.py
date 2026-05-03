"""Commit operation CLI setup."""

from argparse import ArgumentParser
from typing import List, Optional

from daggerml._cli.base import apply_help_config, parse_ref


def setup_commit_parser(parser: ArgumentParser) -> None:
    """Setup commit operation parsers and subcommands."""
    apply_help_config(
        parser,
        description="Commit operations: list history, merge/rebase commits, and manage DAGs stored in commits.",
        examples=[
            "dml commit list HEAD --limit 10",
            "dml commit merge commit:abc commit:def --user alice",
            "dml commit rebase commit:abc commit:def --user bob",
            "dml commit get-dag commit:abc mydag",
            "dml commit delete-dag mydag main --user alice",
        ],
    )
    subparsers = parser.add_subparsers(dest="subcommand", metavar="<method>", help="Methods")

    # list subcommand
    list_parser = subparsers.add_parser("list", help="List commit history")
    apply_help_config(
        list_parser,
        description="List commits reachable from a branch/commit ref or the special value HEAD.",
        examples=["dml commit list HEAD --limit 10"],
    )
    list_parser.add_argument("head", help="Branch name, commit ref, revision, or HEAD")
    list_parser.add_argument("--limit", type=int, help="Maximum number of commits to return")
    list_parser.set_defaults(func=execute_commit_list)

    # merge subcommand
    merge_parser = subparsers.add_parser("merge", help="Merge two commits")
    apply_help_config(
        merge_parser,
        description="Create a merge commit from two commit refs.",
        examples=["dml commit merge commit:abc commit:def --user alice"],
    )
    merge_parser.add_argument("commit1", help="Commit ref (commit:<id>)")
    merge_parser.add_argument("commit2", help="Commit ref (commit:<id>)")
    merge_parser.add_argument("--user", required=True, help="Commit author username")
    merge_parser.set_defaults(func=execute_commit_merge)

    merge_head_parser = subparsers.add_parser("merge-head", help="Merge into and advance a branch")
    apply_help_config(
        merge_head_parser,
        description="Merge another commit into a branch head, fast-forwarding when possible.",
        examples=["dml commit merge-head main commit:abc --user alice"],
    )
    merge_head_parser.add_argument("head", help="Branch name")
    merge_head_parser.add_argument("other", help="Commit ref (commit:<id>)")
    merge_head_parser.add_argument("--user", required=True, help="Commit author username")
    merge_head_parser.set_defaults(func=execute_commit_merge_head)

    revert_parser = subparsers.add_parser("revert", help="Revert a commit on a head")
    apply_help_config(revert_parser, description="Apply the inverse of a commit to a branch.")
    revert_parser.add_argument("head", help="Branch name")
    revert_parser.add_argument("commit", help="Commit ref (commit:<id>)")
    revert_parser.add_argument("--user", required=True, help="Commit author username")
    revert_parser.set_defaults(func=execute_commit_revert)

    # rebase subcommand
    rebase_parser = subparsers.add_parser("rebase", help="Rebase source commit onto target")
    apply_help_config(
        rebase_parser,
        description="Rebase a source commit onto a target commit.",
        examples=["dml commit rebase commit:abc commit:def --user bob"],
    )
    rebase_parser.add_argument("source", help="Source commit ref (commit:<id>)")
    rebase_parser.add_argument("target", help="Target commit ref (commit:<id>)")
    rebase_parser.add_argument("--user", required=True, help="Commit author username")
    rebase_parser.set_defaults(func=execute_commit_rebase)

    # get-dag subcommand
    get_dag_parser = subparsers.add_parser("get-dag", help="Get DAG from commit")
    apply_help_config(
        get_dag_parser,
        description="Fetch a named DAG ref stored in a commit.",
        examples=["dml commit get-dag commit:abc mydag"],
    )
    get_dag_parser.add_argument("commit", help="Commit ref (commit:<id>)")
    get_dag_parser.add_argument("name", help="DAG name (string)")
    get_dag_parser.set_defaults(func=execute_commit_get_dag)

    # describe subcommand
    describe_parser = subparsers.add_parser("describe", help="Describe a commit")
    apply_help_config(
        describe_parser,
        description="Describe a commit and return its metadata.",
        examples=["dml commit describe commit:abc"],
    )
    describe_parser.add_argument("commit", help="Commit ref (commit:<id>)")
    describe_parser.set_defaults(func=execute_commit_describe)

    # delete-dag subcommand
    delete_dag_parser = subparsers.add_parser("delete-dag", help="Delete DAG from branch commit")
    apply_help_config(
        delete_dag_parser,
        description="Delete a named DAG from a branch head.",
        examples=["dml commit delete-dag mydag main --user alice"],
    )
    delete_dag_parser.add_argument("name", help="DAG name (string)")
    delete_dag_parser.add_argument("head", help="Branch name")
    delete_dag_parser.add_argument("--user", required=True, help="Commit author username")
    delete_dag_parser.set_defaults(func=execute_commit_delete_dag)


def execute_commit_list(ops_obj, args) -> List[str]:
    """Execute commit list command, return JSON-serializable result."""
    result = ops_obj.list(args.head, args.limit)
    return [str(ref) for ref in result]


def execute_commit_merge(ops_obj, args) -> str:
    """Execute commit merge command, return JSON-serializable result."""
    commit1 = parse_ref(args.commit1)
    commit2 = parse_ref(args.commit2)
    result = ops_obj.merge(commit1, commit2, args.user)
    return str(result)


def execute_commit_merge_head(ops_obj, args) -> str:
    result = ops_obj.merge_into_head(args.head, parse_ref(args.other), args.user)
    return str(result)


def execute_commit_revert(ops_obj, args) -> str:
    result = ops_obj.revert(args.head, parse_ref(args.commit), args.user)
    return str(result)


def execute_commit_rebase(ops_obj, args) -> str:
    """Execute commit rebase command, return JSON-serializable result."""
    source = parse_ref(args.source)
    target = parse_ref(args.target)
    result = ops_obj.rebase(source, target, args.user)
    return str(result)


def execute_commit_get_dag(ops_obj, args) -> Optional[str]:
    """Execute commit get-dag command, return JSON-serializable result."""
    commit = parse_ref(args.commit)
    result = ops_obj.get_dag(commit, args.name)
    return str(result) if result is not None else None


def execute_commit_describe(ops_obj, args) -> dict:
    """Execute commit describe command, return JSON-serializable result."""
    commit = parse_ref(args.commit)
    return ops_obj.describe(commit)


def execute_commit_delete_dag(ops_obj, args) -> str:
    """Execute commit delete-dag command, return JSON-serializable result."""
    # head may be a branch name or a ref string; parse into a Ref when present
    head = parse_ref(args.head) if getattr(args, "head", None) is not None else None
    result = ops_obj.delete_dag(args.name, head, args.user)
    return str(result)
