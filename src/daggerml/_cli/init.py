"""Init command CLI setup."""

from __future__ import annotations

from argparse import ArgumentParser

from daggerml._cli.base import apply_help_config
from daggerml._internal import Dml
from daggerml._internal.dml import InitPayload


def setup_init_parser(parser: ArgumentParser) -> None:
    """Setup init command parser."""
    apply_help_config(
        parser,
        description="Initialize .dml-managed state in the current project directory.",
        examples=[
            "dml init my-project",
            "dml init --remote-project dml://alice/my-project",
            "dml init --project-home /path/to/project my-project",
            "dml --project-home /path/to/project init",
        ],
    )
    parser.add_argument("name", nargs="?", help="Project name")
    parser.add_argument("--owner", default=None, help="Project owner (default: global user)")
    parser.add_argument(
        "--remote-project",
        dest="remote_project",
        default=None,
        help="Explicit remote project URI (dml://owner/project). Mutually exclusive with NAME.",
    )
    parser.add_argument(
        "--remote-root",
        dest="remote_root",
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


def execute_init(args) -> InitPayload:
    """Execute init command."""
    return Dml.init(
        project_home=getattr(args, "project_home", None) or ".",
        name=args.name.strip() if args.name else None,
        owner=getattr(args, "owner", None),
        remote_project=getattr(args, "remote_project", None),
        remote_uri=getattr(args, "remote_root", None),
        user=None,
        config_home=getattr(args, "config_home", None),
        no_hooks=getattr(args, "no_hooks", False),
    )
