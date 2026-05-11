from __future__ import annotations

from argparse import ArgumentParser

from daggerml._cli.base import apply_help_config


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
        parser.add_argument("--branch", dest="branch_name")
    if name in {"pull", "merge", "revert"}:
        parser.add_argument("--user", required=True)
    if name == "push":
        parser.add_argument("--create", action="store_true")
        parser.add_argument("--force", action="store_true")
    if name in {"merge", "revert"}:
        parser.add_argument("revision")
    parser.set_defaults(func=getattr(ProjectAliasHandlers, name.replace("-", "_")))


class ProjectAliasHandlers:
    @staticmethod
    def fetch(dml, args) -> str:
        return str(dml.fetch(args.remote_or_uri, args.branch))

    @staticmethod
    def pull(dml, args) -> str:
        return str(
            dml.pull(
                args.remote_or_uri,
                args.branch,
                branch=args.branch_name,
                user=args.user,
            )
        )

    @staticmethod
    def push(dml, args) -> str:
        return dml.push(
            args.tag,
            branch=args.branch_name,
            create=args.create,
            force=args.force,
        )

    @staticmethod
    def checkout(dml, args) -> dict[str, str | None]:
        return dml.checkout(args.revision)

    @staticmethod
    def merge(dml, args) -> str:
        return str(dml.merge(args.revision, args.branch_name, args.user))

    @staticmethod
    def revert(dml, args) -> str:
        return str(dml.revert(args.revision, args.branch_name, args.user))
