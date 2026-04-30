from __future__ import annotations

from argparse import ArgumentParser
from pathlib import Path

from daggerml._cli.base import apply_help_config, parse_ref
from daggerml._cli.remote import create_s3_client, require_boto3
from daggerml._internal import DmlOps, DmlRepoError
from daggerml._internal.config import DmlConfig, DmlProjectConfig, init_project_layout, run_project_hooks
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
    if name in {"fetch", "pull"}:
        parser.add_argument("remote_or_uri")
        parser.add_argument("branch", nargs="?")
    if name == "push":
        parser.add_argument("tag", nargs="?", help="Optional tag name to push")
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
    cfg = DmlConfig.resolve(scope="global")
    parsed = RemoteOps.parse_dml_uri(args.uri, require_identifier=False)
    if parsed.tag is not None and _looks_like_commit_id(parsed.tag):
        raise DmlRepoError(
            "Clone direct-commit targets are not supported yet; fetch currently supports only branch/tag refs"
        )
    local_branch = args.branch or parsed.branch or cfg.default_branch
    target = parsed.branch or parsed.tag or local_branch
    if target is None:
        raise DmlRepoError("Clone target could not be resolved")

    project_dir = Path(parsed.project)
    if project_dir.exists():
        raise FileExistsError(f"Project directory exists: {project_dir}")

    remote_root = f"s3://{args.bucket}/{args.prefix.strip('/')}" if args.prefix.strip("/") else f"s3://{args.bucket}"
    project = DmlProjectConfig(
        name=parsed.project,
        owner=parsed.owner,
        branch=local_branch,
        remote_uri=remote_root,
    )

    boto3 = require_boto3()
    s3_client = create_s3_client(boto3)

    project_dir.mkdir()
    init_project_layout(project_dir, project)

    with DmlOps.create(str(project_dir), remote_root=project.remote_uri, branch=local_branch) as ops:
        remote_target = f"{project.uri}#{target}" if parsed.tag is None else f"{project.uri}@{target}"
        ops.fetch_project(remote_target, None, s3_client=s3_client)
        checkout_result = ops.checkout_project(target)

    run_project_hooks(
        "post-clone",
        cfg.hooks.post_clone,
        project_dir=project_dir,
        project=project,
        config_home=cfg.config_home,
        remote_name="origin",
        no_hooks=args.no_hooks,
    )

    return {
        "project_dir": str(project_dir),
        "head": checkout_result["head"],
        "mode": str(checkout_result["mode"]),
        "commit": str(checkout_result["commit"]),
        "message": str(checkout_result["message"]),
    }


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
            args.tag,
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
