from __future__ import annotations

from argparse import ArgumentParser

from daggerml._cli.base import apply_help_config
from daggerml._internal import DmlRepoError


def setup_admin_parser(parser: ArgumentParser) -> None:
    apply_help_config(
        parser,
        description="Administrative and maintenance commands.",
        examples=[
            "dml admin index list",
            "dml admin cache invalidate ck1 ck2",
            "dml admin remote list --owner alice",
            "dml admin remote gc",
            "dml admin gc --dry-run",
        ],
    )
    subparsers = parser.add_subparsers(dest="subcommand", metavar="<command>", help="Admin commands", required=True)

    index_parser = subparsers.add_parser("index", help="Inspect indexes")
    setup_admin_index_parser(index_parser)

    cache_parser = subparsers.add_parser("cache", help="Invalidate cache entries")
    setup_admin_cache_parser(cache_parser)

    remote_parser = subparsers.add_parser("remote", help="Inspect or maintain remotes")
    setup_admin_remote_parser(remote_parser)

    gc_parser = subparsers.add_parser("gc", help="Garbage-collect local objects")
    apply_help_config(gc_parser, description="Garbage-collect unreachable local objects.")
    gc_parser.add_argument("--dry-run", action="store_true")
    gc_parser.set_defaults(op="admin", subcommand="gc", func=execute_admin_gc)


def setup_admin_index_parser(parser: ArgumentParser) -> None:
    subparsers = parser.add_subparsers(dest="method", metavar="<method>", help="Index methods", required=True)

    list_parser = subparsers.add_parser("list", help="List indexes")
    list_parser.set_defaults(op="admin", subcommand="index", method="list", func=execute_admin_index_list)

    get_parser = subparsers.add_parser("get", help="Get one index")
    get_parser.add_argument("index_id")
    get_parser.set_defaults(op="admin", subcommand="index", method="get", func=execute_admin_index_get)

    delete_parser = subparsers.add_parser("delete", help="Delete one index")
    delete_parser.add_argument("index_id")
    delete_parser.set_defaults(op="admin", subcommand="index", method="delete", func=execute_admin_index_delete)


def setup_admin_cache_parser(parser: ArgumentParser) -> None:
    subparsers = parser.add_subparsers(dest="method", metavar="<method>", help="Cache methods", required=True)

    invalidate_parser = subparsers.add_parser("invalidate", help="Invalidate exact cache keys")
    invalidate_parser.add_argument("cache_keys", nargs="+")
    invalidate_parser.set_defaults(
        op="admin",
        subcommand="cache",
        method="invalidate",
        func=execute_admin_cache_invalidate,
    )


def setup_admin_remote_parser(parser: ArgumentParser) -> None:
    subparsers = parser.add_subparsers(dest="method", metavar="<method>", help="Remote methods", required=True)

    list_parser = subparsers.add_parser("list", help="List remote projects or refs")
    list_parser.add_argument("project", nargs="?", default=None)
    list_parser.add_argument("--owner", default=None)
    list_parser.set_defaults(op="admin", subcommand="remote", method="list", func=execute_admin_remote_list)

    gc_parser = subparsers.add_parser("gc", help="Run remote maintenance")
    gc_parser.set_defaults(op="admin", subcommand="remote", method="gc", func=execute_admin_remote_gc)


def execute_admin_index_list(dml, _args) -> dict[str, object]:
    return dml.admin.index.list()


def execute_admin_index_get(dml, args) -> dict[str, object]:
    return dml.admin.index.get(args.index_id)


def execute_admin_index_delete(dml, args) -> dict[str, object]:
    return dml.admin.index.delete(args.index_id)


def execute_admin_cache_invalidate(dml, args) -> dict[str, object]:
    return dml.admin.cache.invalidate(list(args.cache_keys))


def execute_admin_remote_list(dml, args) -> dict[str, object]:
    if args.project is not None and args.owner is not None:
        raise DmlRepoError("--owner cannot be combined with a specific project")
    return dml.admin.remote.list(args.project, owner=args.owner)


def execute_admin_remote_gc(dml, _args) -> dict[str, object]:
    return dml.admin.remote.gc()


def execute_admin_gc(dml, args) -> dict[str, object]:
    return dml.admin.gc(dry_run=args.dry_run)
