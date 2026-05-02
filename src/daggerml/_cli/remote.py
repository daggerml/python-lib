from __future__ import annotations

import importlib
from argparse import ArgumentParser
from typing import Any

from daggerml._cli.base import apply_help_config
from daggerml._internal import DmlOps
from daggerml._internal.types import DmlRepoError


def setup_remote_parser(parser: ArgumentParser) -> None:
    """Setup remote operation parsers and subcommands."""
    apply_help_config(
        parser,
        description="Remote operations backed by S3 (requires boto3 at runtime for remote commands).",
        examples=[
            "dml --remote-root s3://bucket/project remote push head:main",
            "dml --remote-root s3://bucket/project remote pull tags/main/v1.json",
        ],
    )
    subparsers = parser.add_subparsers(dest="method", metavar="<method>", help="Methods", required=True)

    setup_remote_push_parser(subparsers.add_parser("push", help="Push a branch to remote"))
    setup_remote_fetch_parser(subparsers.add_parser("fetch", help="Fetch a project URI into local tracking refs"))
    setup_remote_pull_branch_parser(
        subparsers.add_parser("pull-branch", help="Fetch and merge a project branch")
    )
    setup_remote_push_branch_parser(
        subparsers.add_parser(
            "push-branch", help="Push a local branch to a project branch"
        )
    )
    setup_remote_pull_parser(subparsers.add_parser("pull", help="Pull a ref path from remote"))
    setup_remote_list_parser(subparsers.add_parser("list", help="List remote refs under a prefix"))
    setup_remote_prune_parser(subparsers.add_parser("prune", help="Prune remote io/invoke transport blobs"))
    setup_remote_gc_parser(subparsers.add_parser("gc", help="Run remote garbage collection"))


def setup_remote_push_parser(parser: ArgumentParser) -> None:
    """Configure arguments for `dml remote push`."""
    apply_help_config(
        parser,
        description="Publish a local branch as a tag ref in remote storage.",
        examples=[
            "dml --remote-root s3://bucket/project remote push main",
        ],
    )
    parser.add_argument("branch", help="Branch name to push")
    parser.set_defaults(method="push", func=execute_remote_push)


def setup_remote_pull_parser(parser: ArgumentParser) -> None:
    """Configure arguments for `dml remote pull`."""
    apply_help_config(
        parser,
        description="Pull a ref path from remote storage into the local repo.",
        examples=["dml --remote-root s3://bucket/project remote pull tags/main/v1.json"],
    )
    parser.add_argument("ref_path", help="Remote ref path (e.g. tags/<name>/<version>.json)")
    parser.set_defaults(method="pull", func=execute_remote_pull)


def setup_remote_fetch_parser(parser: ArgumentParser) -> None:
    apply_help_config(parser, description="Fetch dml://<owner>/<project>#<branch-or-tag> into local tracking refs.")
    parser.add_argument("uri")
    parser.set_defaults(method="fetch", func=execute_remote_fetch)


def setup_remote_pull_branch_parser(parser: ArgumentParser) -> None:
    apply_help_config(parser, description="Fetch a DML URI and merge it into the selected local branch.")
    parser.add_argument("uri")
    parser.add_argument("--branch", dest="branch_name", default="main")
    parser.add_argument("--user", required=True)
    parser.set_defaults(method="pull-branch", func=execute_remote_pull_branch)


def setup_remote_push_branch_parser(parser: ArgumentParser) -> None:
    apply_help_config(parser, description="Push a local branch to dml://<owner>/<project>#<branch>.")
    parser.add_argument("uri")
    parser.add_argument("--branch", dest="branch_name", default="main")
    parser.add_argument("--create", action="store_true")
    parser.add_argument("--force", action="store_true")
    parser.set_defaults(method="push-branch", func=execute_remote_push_branch)


def setup_remote_list_parser(parser: ArgumentParser) -> None:
    """Configure arguments for `dml remote list`."""
    apply_help_config(
        parser,
        description="List remote refs under a prefix.",
        examples=["dml --remote-root s3://bucket/project remote list tags"],
    )
    parser.add_argument("prefix", help="Remote prefix to list (e.g. tags, cache)")
    parser.set_defaults(method="list", func=execute_remote_list)


def setup_remote_prune_parser(parser: ArgumentParser) -> None:
    """Configure arguments for `dml remote prune`."""
    apply_help_config(
        parser,
        description="Prune aged remote io/invoke transport blobs.",
        examples=["dml --remote-root s3://bucket/project remote prune"],
    )
    parser.set_defaults(method="prune", func=execute_remote_prune)


def setup_remote_gc_parser(parser: ArgumentParser) -> None:
    """Configure arguments for `dml remote gc`."""
    apply_help_config(
        parser,
        description="Run remote garbage collection.",
        examples=["dml --remote-root s3://bucket/project remote gc --min-age 0"],
    )
    parser.add_argument(
        "--min-age",
        type=int,
        default=24 * 3600,
        help="Minimum age in seconds for CAS objects to be eligible for deletion",
    )
    parser.set_defaults(method="gc", func=execute_remote_gc)


def require_boto3() -> Any:
    """Import boto3 only when a remote command executes."""
    try:
        return importlib.import_module("boto3")
    except ImportError as exc:
        raise DmlRepoError("Remote commands require boto3; install boto3 to continue") from exc


def create_s3_client(boto3_module: Any) -> Any:
    """Create a boto3 S3 client using default credential resolution."""
    return boto3_module.client("s3")


def get_remote_ops(ops: DmlOps, s3_client: Any) -> Any:
    """Get remote operations from DmlOps instance."""
    if not ops.remote_root:
        raise DmlRepoError("Remote URI required; pass --remote-root or set DML_REMOTE_URI")
    return ops.remote(client=s3_client)


def execute_remote_push(ops, args) -> str:
    """Execute `dml remote push`."""
    boto3 = require_boto3()
    s3_client = create_s3_client(boto3)
    remote_ops = get_remote_ops(ops, s3_client)
    return remote_ops.push(args.branch)


def execute_remote_pull(ops, args) -> None:
    """Execute `dml remote pull`."""
    boto3 = require_boto3()
    s3_client = create_s3_client(boto3)
    remote_ops = get_remote_ops(ops, s3_client)
    remote_ops.pull(args.ref_path)
    return None


def execute_remote_fetch(ops, args) -> str:
    boto3 = require_boto3()
    s3_client = create_s3_client(boto3)
    remote_ops = get_remote_ops(ops, s3_client)
    return str(remote_ops.fetch_uri(args.uri))


def execute_remote_pull_branch(ops, args) -> str:
    boto3 = require_boto3()
    s3_client = create_s3_client(boto3)
    remote_ops = get_remote_ops(ops, s3_client)
    return str(remote_ops.pull_uri_into_branch(args.uri, args.branch_name, user=args.user))


def execute_remote_push_branch(ops, args) -> str:
    boto3 = require_boto3()
    s3_client = create_s3_client(boto3)
    remote_ops = get_remote_ops(ops, s3_client)
    return remote_ops.push_project_branch(args.uri, args.branch_name, create=args.create, force=args.force)


def execute_remote_list(ops, args) -> list[dict]:
    """Execute `dml remote list`."""
    boto3 = require_boto3()
    s3_client = create_s3_client(boto3)
    remote_ops = get_remote_ops(ops, s3_client)
    return remote_ops.list(args.prefix)


def execute_remote_prune(ops, args) -> int:
    """Execute `dml remote prune`."""
    boto3 = require_boto3()
    s3_client = create_s3_client(boto3)
    remote_ops = get_remote_ops(ops, s3_client)
    return remote_ops.prune()


def execute_remote_gc(ops, args) -> dict[str, int]:
    """Execute `dml remote gc`."""
    boto3 = require_boto3()
    s3_client = create_s3_client(boto3)
    remote_ops = get_remote_ops(ops, s3_client)
    return remote_ops.gc(min_age_seconds=args.min_age)
