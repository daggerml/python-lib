from __future__ import annotations

from argparse import ArgumentParser
from pathlib import Path
from typing import Any

from daggerml._cli.base import apply_help_config, parse_ref
from daggerml._cli.remote import create_s3_client, require_boto3
from daggerml._config import (
    DmlGlobalConfig,
    DmlProjectConfig,
    DmlRemoteProjectConfig,
    global_config_home,
    init_project_layout,
    run_project_hooks,
)
from daggerml._internal import DmlOps
from daggerml._internal.ops.remote import RemoteOps


def setup_clone_parser(parser: ArgumentParser) -> None:
    apply_help_config(parser, description="Clone a remote DML project branch into a project directory.")
    parser.add_argument("uri")
    parser.add_argument("--bucket", required=True)
    parser.add_argument("--prefix", default="dml")
    parser.add_argument("--branch", default=None)
    parser.add_argument("--no-hooks", action="store_true")
    parser.set_defaults(func=execute_clone)


def setup_project_alias_parser(parser: ArgumentParser, name: str) -> None:
    apply_help_config(parser, description=f"Git-like project {name} operation.")
    if name in {"fetch", "pull", "push"}:
        parser.add_argument("remote_or_uri")
        parser.add_argument("branch", nargs="?")
    if name in {"pull", "merge", "revert", "push"}:
        parser.add_argument("--head", default="head:main")
    if name in {"pull", "merge", "revert"}:
        parser.add_argument("--user", required=True)
    if name == "push":
        parser.add_argument("--create", action="store_true")
        parser.add_argument("--force", action="store_true")
    if name in {"merge", "revert"}:
        parser.add_argument("commitish")
    parser.set_defaults(func=getattr(ProjectAliasHandlers, name.replace("-", "_")))


def _remote_uri(project: DmlProjectConfig, remote_or_uri: str, branch: str | None) -> str:
    if remote_or_uri.startswith("dml://"):
        if "#" in remote_or_uri or "@" in remote_or_uri:
            return remote_or_uri
        return f"{remote_or_uri}#{branch or project.branch}"
    remote = project.remotes[remote_or_uri]
    return f"{remote.uri}#{branch or project.branch}"


def _ops_remote(ops: DmlOps, project: DmlProjectConfig, remote_or_uri: str, s3_client: Any) -> RemoteOps:
    remote = None if remote_or_uri.startswith("dml://") else project.remotes[remote_or_uri]
    if remote is None:
        bucket, prefix = ops._split_remote_root(ops.remote_root)
    else:
        bucket, prefix = remote.bucket, remote.prefix
    return RemoteOps(_db=ops._db, bucket=bucket, prefix=prefix, client=s3_client)  # type: ignore[arg-type]


def execute_clone(args) -> dict[str, str]:
    parsed = RemoteOps.parse_dml_uri(args.uri, require_identifier=False)
    branch = args.branch or parsed.branch or DmlGlobalConfig.load().default_branch or "main"
    project_dir = Path(parsed.project)
    if project_dir.exists():
        raise FileExistsError(f"Project directory exists: {project_dir}")
    project = DmlProjectConfig(
        name=parsed.project,
        owner=parsed.owner,
        branch=branch,
        remotes={
            "origin": DmlRemoteProjectConfig(
                uri=f"dml://{parsed.owner}/{parsed.project}",
                bucket=args.bucket,
                prefix=args.prefix,
            )
        },
    )
    project_dir.mkdir()
    init_project_layout(project_dir, project)
    with DmlOps.create(str(project_dir), remote_root=f"s3://{args.bucket}/{args.prefix}", branch=branch) as ops:
        boto3 = require_boto3()
        remote_ops = _ops_remote(ops, project, "origin", create_s3_client(boto3))
        fetched = remote_ops.fetch_uri(f"{project.uri}#{branch}")
        ops.head().advance(parse_ref(f"head:{branch}"), fetched)
    global_cfg = DmlGlobalConfig.load()
    run_project_hooks(
        "post-clone",
        global_cfg.post_clone,
        project_dir=project_dir,
        project=project,
        config_home=global_config_home(),
        remote_name="origin",
        no_hooks=args.no_hooks,
    )
    return {"project_dir": str(project_dir), "head": f"head:{branch}"}


class ProjectAliasHandlers:
    @staticmethod
    def fetch(ops: DmlOps, args) -> str:
        project = DmlProjectConfig.load(".")
        uri = _remote_uri(project, args.remote_or_uri, args.branch)
        boto3 = require_boto3()
        return str(_ops_remote(ops, project, args.remote_or_uri, create_s3_client(boto3)).fetch_uri(uri))

    @staticmethod
    def pull(ops: DmlOps, args) -> str:
        project = DmlProjectConfig.load(".")
        uri = _remote_uri(project, args.remote_or_uri, args.branch)
        boto3 = require_boto3()
        remote_ops = _ops_remote(ops, project, args.remote_or_uri, create_s3_client(boto3))
        return str(remote_ops.pull_uri_into_head(uri, parse_ref(args.head), user=args.user))

    @staticmethod
    def push(ops: DmlOps, args) -> str:
        project = DmlProjectConfig.load(".")
        uri = _remote_uri(project, args.remote_or_uri, args.branch)
        boto3 = require_boto3()
        return _ops_remote(ops, project, args.remote_or_uri, create_s3_client(boto3)).push_project_branch(
            uri, parse_ref(args.head), create=args.create, force=args.force
        )

    @staticmethod
    def merge(ops: DmlOps, args) -> str:
        commit_ops = ops.commit()
        other = commit_ops.resolve_commitish(args.commitish, project_dir=".")
        return str(commit_ops.merge_into_head(parse_ref(args.head), other, args.user))

    @staticmethod
    def revert(ops: DmlOps, args) -> str:
        commit_ops = ops.commit()
        commit = commit_ops.resolve_commitish(args.commitish, project_dir=".")
        return str(commit_ops.revert(parse_ref(args.head), commit, args.user))
