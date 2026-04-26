"""Init command CLI setup."""

from __future__ import annotations

import re
from argparse import ArgumentParser
from pathlib import Path

from daggerml._cli.base import apply_help_config
from daggerml._config import (
    DmlConfig,
    DmlGlobalConfig,
    DmlProjectConfig,
    global_config_home,
    init_project_layout,
    run_project_hooks,
)
from daggerml._internal import DmlOps


def _default_owner(value: str) -> str:
    value = value.split("@", 1)[0].lower()
    value = re.sub(r"[^a-z0-9._-]+", "-", value).strip("-._")
    return value or "dml"


def setup_init_parser(parser: ArgumentParser) -> None:
    """Setup init command parser."""
    apply_help_config(
        parser,
        description="Create a DML project directory with .dml-managed state.",
        examples=[
            "dml init my-repo",
            "dml init my-project",
            "dml init --here my-project",
        ],
    )
    parser.add_argument("name", nargs="?", help="Project name")
    parser.add_argument("--here", action="store_true", help="Initialize the current directory")
    parser.add_argument("--owner", default=None, help="Project owner (default: global user)")
    parser.add_argument("--branch", default=None, help="Initial branch (default: global default branch or main)")
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
    here = getattr(args, "here", False)
    if not repo_name and not args.repo:
        raise ValueError("NAME is required when --repo is not provided")
    if repo_name and ("/" in repo_name or "\\" in repo_name):
        raise ValueError("Repository NAME must not contain path separators")

    cfg = DmlConfig.resolve(
        explicit={
            "repo": args.repo,
        }
    )
    config_home = Path(str(getattr(args, "config_home", None) or global_config_home()))
    global_cfg = DmlGlobalConfig.load(config_home)
    explicit_owner = getattr(args, "owner", None)
    owner = explicit_owner or _default_owner(str(cfg.user or global_cfg.user or "dml"))
    if not owner:
        raise ValueError("Project owner is required; pass --owner or set DML_USER/global [user].name")
    branch = getattr(args, "branch", None) or global_cfg.default_branch or cfg.branch
    if args.repo:
        repo_path = Path(args.repo)
        project_name = repo_name or repo_path.name
    else:
        project_name = str(repo_name)
        repo_path = Path.cwd() if here else Path.cwd() / project_name
    if not here and repo_path.exists():
        raise FileExistsError(f"Project directory exists: {repo_path}. Use 'dml init --here {repo_name}' inside it.")
    repo_path.mkdir(parents=True, exist_ok=here)
    project = DmlProjectConfig(name=project_name, owner=owner, branch=branch)
    db_path = init_project_layout(repo_path, project)
    remote_root = cfg.remote.root

    with DmlOps.create(str(repo_path), remote_root=remote_root, branch=branch):
        pass
    run_project_hooks(
        "post-init",
        global_cfg.post_init,
        project_dir=repo_path,
        project=project,
        config_home=config_home,
        no_hooks=getattr(args, "no_hooks", False),
    )

    return {
        "name": repo_name,
        "repo_path": str(repo_path),
        "db_path": str(db_path),
        "head": f"head:{branch}",
    }
