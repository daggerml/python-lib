from __future__ import annotations

from argparse import ArgumentParser

from daggerml._cli.base import apply_help_config
from daggerml._internal.config import DmlConfig
from daggerml._internal.ops.config import SCOPE_GLOBAL, SCOPE_LOCAL, ConfigOps, render_config_output
from daggerml._internal.types import DmlRepoError


def setup_config_parser(parser: ArgumentParser) -> None:
    apply_help_config(
        parser,
        description="Get or set supported DML configuration values.",
        examples=[
            "dml config project.uri",
            "dml config project.uri dml://alice/demo#main",
            "dml config --global user alice@example",
            "dml config --global hooks.post-init 'echo hi' 'echo done'",
        ],
    )
    parser.add_argument("--global", dest="global_scope", action="store_true", help="Use global config scope")
    parser.add_argument("key", help="Config key")
    parser.add_argument("value", nargs="*", help="Config value(s) for set")
    parser.set_defaults(func=execute_config)


def execute_config(args) -> str:
    cfg = DmlConfig.resolve(
        scope="global" if args.global_scope else "project/runtime",
        explicit={"project.home": args.repo},
    )
    ops = ConfigOps(project_home=cfg.project.home, config_home=cfg.config_home)
    scope = SCOPE_GLOBAL if args.global_scope else SCOPE_LOCAL
    if args.value:
        value = ops.set(args.key, list(args.value), scope=scope)
        if isinstance(value, list):
            return ""
        return ""
    value = ops.get(args.key, scope=scope)
    if value is None:
        raise DmlRepoError(f"Config key not set: {args.key}")
    return render_config_output(value)
