"""Init command CLI setup."""

from __future__ import annotations

from argparse import ArgumentParser
from pathlib import Path
from typing import cast

from daggerml._cli.base import apply_help_config
from daggerml._config import DmlConfig
from daggerml._internal import DmlOps
from daggerml._internal.types import DEFAULT_HEAD


def setup_init_parser(parser: ArgumentParser) -> None:
    """Setup init command parser."""
    apply_help_config(
        parser,
        description="Create a new named repository under a DML config directory.",
        examples=[
            "dml init my-repo",
            "dml init --config-dir ~/.config/dml my-repo",
            "dml --repo /tmp/my-repo-db init",
        ],
    )
    parser.add_argument("name", nargs="?", help="Repository name")
    parser.add_argument(
        "--config-dir",
        default=None,
        help="DML config directory (default: $XDG_CONFIG_HOME/dml or ~/.config/dml)",
    )
    parser.set_defaults(func=execute_init)


def execute_init(args) -> dict[str, str | None]:
    """Execute init command."""
    repo_name = args.name.strip() if args.name else None

    cfg = DmlConfig.resolve(
        explicit={
            "repo": args.repo,
            "config_dir": args.config_dir,
        }
    )
    config_dir = Path(cast(str, cfg.config_dir))
    if cfg.repo:
        repo_path = Path(cfg.repo)
    else:
        if not repo_name:
            raise ValueError("NAME is required when --repo is not provided")
        if "/" in repo_name or "\\" in repo_name:
            raise ValueError("Repository NAME must not contain path separators")
        repo_path = config_dir / repo_name

    remote_root = cfg.remote.root
    if remote_root is None:
        raise ValueError("Remote root is required")

    with DmlOps.create(str(repo_path), remote_root=remote_root):
        pass

    return {
        "name": repo_name,
        "config_dir": str(config_dir),
        "repo_path": str(repo_path),
        "head": DEFAULT_HEAD.to,
    }
