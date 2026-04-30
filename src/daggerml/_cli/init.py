"""Init command CLI setup."""

from __future__ import annotations

import re
from argparse import ArgumentParser

from daggerml._cli.base import apply_help_config
from daggerml._internal import DmlOps
from daggerml._internal.config import DmlConfig


def _default_owner(value: str) -> str:
    value = value.split("@", 1)[0].lower()
    value = re.sub(r"[^a-z0-9._-]+", "-", value).strip("-._")
    return value or "dml"


def setup_init_parser(parser: ArgumentParser) -> None:
    """Setup init command parser."""
    apply_help_config(
        parser,
        description="Initialize .dml-managed state in the current project directory.",
        examples=[
            "dml init my-project",
            "dml init --repo /path/to/project my-project",
            "dml --repo /path/to/project init",
        ],
    )
    parser.add_argument("name", nargs="?", help="Project name")
    parser.add_argument("--owner", default=None, help="Project owner (default: global user)")
    parser.add_argument("--branch", default=None, help="Initial branch (default: global default branch or main)")
    parser.add_argument(
        "--project-uri",
        default=None,
        help="Explicit project URI (dml://owner/project#branch). Overrides owner/name/branch derivation.",
    )
    parser.add_argument(
        "--remote-uri",
        default=None,
        help="Remote root URI (s3://bucket or s3://bucket/prefix).",
    )
    parser.add_argument("--no-hooks", action="store_true", help="Skip post-init hooks")
    parser.add_argument(
        "--config-home",
        default=None,
        help="Global DML config home (default: $DML_CONFIG_HOME, $XDG_CONFIG_HOME/dml, or ~/.config/dml)",
    )
    parser.set_defaults(func=execute_init)


def execute_init(args) -> dict[str, str | None]:
    """Execute init command."""
    repo_name = args.name.strip() if args.name else None
    if not repo_name and not args.repo:
        raise ValueError("NAME is required when --repo is not provided")
    if repo_name and ("/" in repo_name or "\\" in repo_name):
        raise ValueError("Repository NAME must not contain path separators")

    cfg = DmlConfig.resolve(
        scope="global",
        explicit={
            "project.home": args.repo,
            "config_home": getattr(args, "config_home", None),
        },
    )
    owner = getattr(args, "owner", None) or _default_owner(str(cfg.user or "dml"))
    branch = getattr(args, "branch", None) or cfg.default_branch
    init_result = DmlOps.init(
        path=args.repo,
        name=repo_name,
        owner=owner,
        branch=branch,
        project_uri=getattr(args, "project_uri", None),
        remote_uri=getattr(args, "remote_uri", None),
        user=cfg.user,
        config_home=getattr(args, "config_home", None),
        no_hooks=getattr(args, "no_hooks", False),
    )
    return init_result
