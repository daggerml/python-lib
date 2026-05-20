import inspect
from typing import Annotated, get_args, get_origin, get_type_hints

from daggerml._internal.dml import (
    Dml,
    _AdminCacheNamespace,
    _AdminNamespace,
    _AdminRemoteNamespace,
    _ConfigNamespace,
    _DagNamespace,
    _RuntimeNamespace,
)


def _assert_docstrings(cls, method_names):
    assert inspect.getdoc(cls)
    for method_name in method_names:
        assert inspect.getdoc(getattr(cls, method_name)), f"Missing docstring for {cls.__name__}.{method_name}"


def _assert_annotated_help(fn, parameter_names):
    hints = get_type_hints(fn, include_extras=True)
    for parameter_name in parameter_names:
        annotation = hints[parameter_name]
        assert get_origin(annotation) is Annotated, f"{fn.__qualname__}.{parameter_name} is not Annotated"
        extras = get_args(annotation)[1:]
        assert extras, f"{fn.__qualname__}.{parameter_name} is missing help metadata"
        assert isinstance(extras[0], str)
        assert extras[0]


def test_public_dml_classes_and_methods_have_docstrings():
    _assert_docstrings(
        Dml,
        [
            "__init__",
            "status",
            "branch",
            "log",
            "show",
            "diff",
            "checkout",
            "fetch",
            "pull",
            "push",
            "merge",
            "revert",
            "init",
        ],
    )
    _assert_docstrings(_ConfigNamespace, ["get", "set", "show"])
    _assert_docstrings(
        _RuntimeNamespace,
        [
            "create",
            "get_node",
            "get_argv",
            "put_literal",
            "put_import",
            "set_node_name",
            "start_fn",
            "commit",
            "list",
            "describe",
            "delete",
            "cancel",
        ],
    )
    _assert_docstrings(
        _DagNamespace,
        ["get", "describe_node", "get_node", "checkout", "delete"],
    )
    _assert_docstrings(_AdminNamespace, ["gc"])
    _assert_docstrings(_AdminCacheNamespace, ["invalidate"])
    _assert_docstrings(_AdminRemoteNamespace, ["list", "gc"])


def test_public_dml_annotations_include_help_metadata():
    _assert_annotated_help(Dml.__init__, ["project_home", "remote_root", "user", "config_home"])
    _assert_annotated_help(Dml.branch, ["remote"])
    _assert_annotated_help(Dml.log, ["revision", "limit"])
    _assert_annotated_help(Dml.show, ["revision"])
    _assert_annotated_help(Dml.diff, ["left", "right"])
    _assert_annotated_help(Dml.checkout, ["revision"])
    _assert_annotated_help(Dml.fetch, ["remote_or_uri", "branch"])
    _assert_annotated_help(Dml.pull, ["remote_or_uri", "remote_branch", "branch", "user"])
    _assert_annotated_help(Dml.push, ["tag", "branch", "create", "force"])
    _assert_annotated_help(Dml.merge, ["revision", "branch", "user"])
    _assert_annotated_help(Dml.revert, ["revision", "branch", "user"])
    _assert_annotated_help(
        Dml.init,
        ["project_home", "remote_root", "user", "config_home", "remote_project"],
    )

    _assert_annotated_help(_ConfigNamespace.get, ["key", "scope"])
    _assert_annotated_help(_ConfigNamespace.set, ["key", "value", "scope"])
    _assert_annotated_help(_ConfigNamespace.show, ["contrib"])

    _assert_annotated_help(_RuntimeNamespace.create, ["head", "commit", "argv_ptr", "index_id"])
    _assert_annotated_help(_RuntimeNamespace.get_node, ["index_id", "name"])
    _assert_annotated_help(_RuntimeNamespace.get_argv, ["index_id"])
    _assert_annotated_help(_RuntimeNamespace.put_literal, ["index_id", "value", "name"])
    _assert_annotated_help(_RuntimeNamespace.put_import, ["index_id", "dag", "node", "name"])
    _assert_annotated_help(_RuntimeNamespace.set_node_name, ["index_id", "name", "node"])
    _assert_annotated_help(_RuntimeNamespace.start_fn, ["index_id", "argv", "kwargv", "name"])
    _assert_annotated_help(_RuntimeNamespace.commit, ["index_id", "value", "head", "message", "dag_name"])
    _assert_annotated_help(_RuntimeNamespace.describe, ["index_id"])
    _assert_annotated_help(_RuntimeNamespace.delete, ["index_id"])
    _assert_annotated_help(_RuntimeNamespace.cancel, ["index_id"])

    _assert_annotated_help(_DagNamespace.get, ["value", "revision"])
    _assert_annotated_help(_DagNamespace.describe_node, ["node", "dag", "revision"])
    _assert_annotated_help(_DagNamespace.get_node, ["node", "dag", "revision"])
    _assert_annotated_help(_DagNamespace.checkout, ["revision", "dag_name", "branch", "target_name", "replace", "user"])
    _assert_annotated_help(_DagNamespace.delete, ["name", "branch", "user"])

    _assert_annotated_help(_AdminNamespace.gc, ["dry_run"])
    _assert_annotated_help(_AdminCacheNamespace.invalidate, ["cache_keys"])
    _assert_annotated_help(_AdminRemoteNamespace.list, ["project", "owner"])
    _assert_annotated_help(_AdminRemoteNamespace.gc, ["min_age_seconds", "malformed"])
