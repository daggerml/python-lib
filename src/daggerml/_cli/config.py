from __future__ import annotations

from argparse import ArgumentParser

from daggerml._cli.base import apply_help_config
from daggerml._internal import DmlRepoError


def setup_config_parser(parser: ArgumentParser) -> None:
    apply_help_config(
        parser,
        description="Inspect or update DML configuration.",
        examples=[
            "dml config show",
            "dml config show --contrib",
            "dml config get project.uri",
            "dml config set project.uri dml://alice/demo",
            "dml config set --global user alice@example",
        ],
    )
    subparsers = parser.add_subparsers(dest="method", metavar="<method>", help="Config methods", required=True)

    show_parser = subparsers.add_parser("show", help="Show resolved config")
    apply_help_config(show_parser, description="Show resolved config as JSON.")
    show_parser.add_argument("--contrib", action="store_true", help="Include contrib plugin status")
    show_parser.set_defaults(op="config", method="show", func=execute_config_show)

    get_parser = subparsers.add_parser("get", help="Get one config value")
    apply_help_config(
        get_parser,
        description="Read one config value and print it as plain text.",
        examples=["dml config get project.uri", "dml config get --global user"],
    )
    get_parser.add_argument("--global", dest="global_scope", action="store_true", help="Use global config scope")
    get_parser.add_argument("key", help="Config key")
    get_parser.set_defaults(op="config", method="get", func=execute_config_get, raw_output=True)

    set_parser = subparsers.add_parser("set", help="Set one config value")
    apply_help_config(
        set_parser,
        description="Set one config value.",
        examples=["dml config set project.uri dml://alice/demo", "dml config set --global user alice@example"],
    )
    set_parser.add_argument("--global", dest="global_scope", action="store_true", help="Use global config scope")
    set_parser.add_argument("key", help="Config key")
    set_parser.add_argument("value", nargs="+", help="Config value(s)")
    set_parser.set_defaults(op="config", method="set", func=execute_config_set, raw_output=True)


def _scope(args) -> str:
    return "global" if getattr(args, "global_scope", False) else "local"


def execute_config_show(dml, args) -> dict[str, object]:
    return dml.config.show(contrib=getattr(args, "contrib", False))


def execute_config_get(dml, args) -> str:
    value = dml.config.get(args.key, scope=_scope(args))
    if value is None:
        raise DmlRepoError(f"Config key not set: {args.key}")
    return value


def execute_config_set(dml, args) -> str:
    dml.config.set(args.key, list(args.value), scope=_scope(args))
    return ""
