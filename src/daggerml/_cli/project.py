from __future__ import annotations

from argparse import ArgumentParser
from pathlib import Path
from typing import Any

from daggerml._cli.base import apply_help_config, parse_ref
from daggerml._cli.remote import create_s3_client, require_boto3
from daggerml._internal import DmlOps, DmlRepoError
from daggerml._internal._db import DmlDbEnv
from daggerml._internal.config import DmlConfig, DmlProjectConfig, init_project_layout, run_project_hooks
from daggerml._internal.ops.commit import CommitOps
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


def _project_dir(ops: DmlOps) -> str:
    return ops.path


def _load_project_config(ops: DmlOps) -> DmlProjectConfig:
    return DmlProjectConfig.load(_project_dir(ops))


def _remote_uri(project: DmlProjectConfig, remote_or_uri: str, branch: str | None) -> str:
    if remote_or_uri.startswith("dml://"):
        if "#" in remote_or_uri or "@" in remote_or_uri:
            return remote_or_uri
        return f"{remote_or_uri}#{branch or project.branch}"
    if remote_or_uri != "origin":
        raise DmlRepoError(f"Unknown remote: {remote_or_uri}")
    return f"{project.uri}#{branch or project.branch}"


def _ops_remote(ops: DmlOps, s3_client: Any) -> RemoteOps:
    bucket, prefix = ops._split_remote_root(ops.remote_root)
    return RemoteOps(_db=_require_db(ops), bucket=bucket, prefix=prefix, client=s3_client)


def _require_db(ops: DmlOps) -> DmlDbEnv:
    if ops._db is None:
        raise DmlRepoError("Repository is not open")
    return ops._db


def _looks_like_commit_id(value: str) -> bool:
    return len(value) == 64 and all(ch in "0123456789abcdef" for ch in value)


def _checkout_resolved_target(ops: DmlOps, *, revision: str) -> dict[str, str | None]:
    project = _load_project_config(ops)
    commit_ops = CommitOps(_db=_require_db(ops))
    resolution = commit_ops.resolve_revision(
        revision,
        current_branch=project.branch,
        project_dir=_project_dir(ops),
    )
    if resolution.kind == "branch" and resolution.branch is not None:
        next_project = DmlProjectConfig(
            name=project.name,
            owner=project.owner,
            branch=resolution.branch,
            remote_uri=project.remote_uri,
        )
        next_project.save(_project_dir(ops))
        return {
            "commit": str(resolution.commit),
            "mode": "attached",
            "head": f"head:{resolution.branch}",
            "target": resolution.branch,
            "message": f"Checked out branch '{resolution.branch}' (attached)",
        }
    return {
        "commit": str(resolution.commit),
        "mode": "detached",
        "head": None,
        "target": revision,
        "message": f"Checked out {revision!r} in detached scratch mode",
    }


def execute_clone(args) -> dict[str, str | None]:
    cfg = DmlConfig.resolve(scope="global")
    parsed = RemoteOps.parse_dml_uri(args.uri, require_identifier=False)
    if parsed.tag is not None and _looks_like_commit_id(parsed.tag):
        raise DmlRepoError(
            "Clone direct-commit targets are not supported yet; fetch currently supports only branch/tag refs"
        )
    branch = args.branch or parsed.branch or cfg.default_branch
    target = parsed.branch or parsed.tag or branch
    if target is None:
        raise DmlRepoError("Clone target could not be resolved")
    project_dir = Path(parsed.project)
    if project_dir.exists():
        raise FileExistsError(f"Project directory exists: {project_dir}")
    project = DmlProjectConfig(
        name=parsed.project,
        owner=parsed.owner,
        branch=branch,
        remote_uri=f"s3://{args.bucket}/{args.prefix.strip('/')}" if args.prefix.strip("/") else f"s3://{args.bucket}",
    )
    project_dir.mkdir()
    init_project_layout(project_dir, project)
    with DmlOps.create(str(project_dir), remote_root=project.remote_uri, branch=branch) as ops:
        boto3 = require_boto3()
        remote_ops = _ops_remote(ops, create_s3_client(boto3))
        remote_target = f"{project.uri}#{target}" if parsed.tag is None else f"{project.uri}@{target}"
        remote_ops.fetch_uri(remote_target)
        checkout_result = _checkout_resolved_target(ops, revision=target)
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
        project = _load_project_config(ops)
        uri = _remote_uri(project, args.remote_or_uri, args.branch)
        boto3 = require_boto3()
        return str(_ops_remote(ops, create_s3_client(boto3)).fetch_uri(uri))

    @staticmethod
    def pull(ops: DmlOps, args) -> str:
        project = _load_project_config(ops)
        uri = _remote_uri(project, args.remote_or_uri, args.branch)
        boto3 = require_boto3()
        remote_ops = _ops_remote(ops, create_s3_client(boto3))
        return str(remote_ops.pull_uri_into_head(uri, parse_ref(args.head), user=args.user))

    @staticmethod
    def push(ops: DmlOps, args) -> str:
        project = _load_project_config(ops)
        uri = _remote_uri(project, args.remote_or_uri, args.branch)
        boto3 = require_boto3()
        return _ops_remote(ops, create_s3_client(boto3)).push_project_branch(
            uri, parse_ref(args.head), create=args.create, force=args.force
        )

    @staticmethod
    def checkout(ops: DmlOps, args) -> dict[str, str | None]:
        return _checkout_resolved_target(ops, revision=args.revision)

    @staticmethod
    def merge(ops: DmlOps, args) -> str:
        commit_ops = ops.commit()
        other = commit_ops.resolve_revision_ref(args.revision, project_dir=_project_dir(ops))
        return str(commit_ops.merge_into_head(parse_ref(args.head), other, args.user))

    @staticmethod
    def revert(ops: DmlOps, args) -> str:
        commit_ops = ops.commit()
        commit = commit_ops.resolve_revision_ref(args.revision, project_dir=_project_dir(ops))
        return str(commit_ops.revert(parse_ref(args.head), commit, args.user))
