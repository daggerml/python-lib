from __future__ import annotations

from argparse import ArgumentParser

from daggerml._cli.base import apply_help_config
from daggerml._internal.config import DmlConfig


def execute_status(args) -> dict[str, object]:
    cfg = DmlConfig.resolve(
        explicit={
            "project.home": args.repo,
            "remote.uri": getattr(args, "remote_root", None),
        }
    )
    return cfg.to_dict()


def setup_status_parser(parser: ArgumentParser) -> None:
    apply_help_config(
        parser,
        description="Show effective current runtime settings (excluding contrib status).",
        examples=[
            "dml status",
            "dml --repo /path/to/repo status",
        ],
    )
    parser.set_defaults(func=execute_status)
