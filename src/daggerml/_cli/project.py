from __future__ import annotations

from argparse import ArgumentParser

from daggerml._cli.base import apply_help_config, parse_ref
from daggerml._cli.remote import create_s3_client, require_boto3
from daggerml._internal import DmlOps, DmlRepoError
from daggerml._internal.ops.remote import RemoteOps


def setup_clone_parser(parser: ArgumentParser) -> None:
    apply_help_config(
        parser,
        description="Clone a remote DML project branch or tag into a project directory.",
        examples=[
            "dml clone dml://alice/demo#main --bucket my-bucket",
            "dml clone dml://alice/demo@v1.0 --bucket my-bucket",
        ],
    )
    parser.add_argument("uri")
    parser.add_argument("--bucket", required=True)
    parser.add_argument("--prefix", default="dml")
    parser.add_argument("--branch", default=None, help="Default local branch name for future branch-attached work")
    parser.add_argument("--no-hooks", action="store_true")
    parser.set_defaults(func=execute_clone)


def setup_project_alias_parser(parser: ArgumentParser, name: str) -> None:
    apply_help_config(parser, description=f"Git-like project {name} operation.")
    if name in {"fetch", "pull", "push"}:
        parser.add_argument("remote_or_uri")
        parser.add_argument("branch", nargs="?")
    if name == "checkout":
        apply_help_config(
            parser,
            description="Checkout a revision target into attached (branch) or detached mode.",
            examples=[
                "dml checkout main",
                "dml checkout v1.0",
                "dml checkout HEAD~1",
            ],
        )
        parser.add_argument("revision")
    if name in {"pull", "merge", "revert", "push"}:
        parser.add_argument("--head", default="head:main")
    if name in {"pull", "merge", "revert"}:
        parser.add_argument("--user", required=True)
    if name == "push":
        parser.add_argument("--create", action="store_true")
        parser.add_argument("--force", action="store_true")
    if name in {"merge", "revert"}:
        parser.add_argument("revision")
    parser.set_defaults(func=getattr(ProjectAliasHandlers, name.replace("-", "_")))


def _looks_like_commit_id(value: str) -> bool:
    return len(value) == 64 and all(ch in "0123456789abcdef" for ch in value)


def execute_clone(args) -> dict[str, str | None]:
    parsed = RemoteOps.parse_dml_uri(args.uri, require_identifier=False)
    if parsed.tag is not None and _looks_like_commit_id(parsed.tag):
        raise DmlRepoError(
            "Clone direct-commit targets are not supported yet; fetch currently supports only branch/tag refs"
        )
    boto3 = require_boto3()
    return DmlOps.clone_project(
        uri=args.uri,
        bucket=args.bucket,
        prefix=args.prefix,
        branch=args.branch,
        no_hooks=args.no_hooks,
        s3_client=create_s3_client(boto3),
    )


class ProjectAliasHandlers:
    @staticmethod
    def fetch(ops: DmlOps, args) -> str:
        boto3 = require_boto3()
        return str(ops.fetch_project(args.remote_or_uri, args.branch, s3_client=create_s3_client(boto3)))

    @staticmethod
    def pull(ops: DmlOps, args) -> str:
        boto3 = require_boto3()
        return str(
            ops.pull_project(
                args.remote_or_uri,
                args.branch,
                head=parse_ref(args.head),
                user=args.user,
                s3_client=create_s3_client(boto3),
            )
        )

    @staticmethod
    def push(ops: DmlOps, args) -> str:
        boto3 = require_boto3()
        return ops.push_project(
            args.remote_or_uri,
            args.branch,
            head=parse_ref(args.head),
            create=args.create,
            force=args.force,
            s3_client=create_s3_client(boto3),
        )

    @staticmethod
    def checkout(ops: DmlOps, args) -> dict[str, str | None]:
        return ops.checkout_project(args.revision)

    @staticmethod
    def merge(ops: DmlOps, args) -> str:
        return str(ops.merge_project(args.revision, parse_ref(args.head), args.user))

    @staticmethod
    def revert(ops: DmlOps, args) -> str:
        return str(ops.revert_project(args.revision, parse_ref(args.head), args.user))
