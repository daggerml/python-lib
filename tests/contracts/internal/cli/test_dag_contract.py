from argparse import Namespace
from unittest.mock import Mock

from daggerml._cli.dag import execute_dag_checkout, execute_dag_delete, execute_dag_get, execute_dag_list


def test_execute_dag_list_delegates_to_dml_namespace():
    dml = Mock()
    dml.dag.list.return_value = {"revision": {"input": "HEAD"}, "dags": {}}

    result = execute_dag_list(dml, Namespace(revision="HEAD"))

    dml.dag.list.assert_called_once_with("HEAD")
    assert result["dags"] == {}


def test_execute_dag_get_delegates_to_dml_namespace():
    dml = Mock()
    dml.dag.get.return_value = {"selector": "train", "dag": {"id": "abc"}}

    result = execute_dag_get(dml, Namespace(selector="train", revision="HEAD"))

    dml.dag.get.assert_called_once_with("train", revision="HEAD")
    assert result["dag"]["id"] == "abc"


def test_execute_dag_checkout_delegates_to_dml_namespace():
    dml = Mock()
    dml.dag.checkout.return_value = "commit:2"

    result = execute_dag_checkout(
        dml,
        Namespace(revision="origin/main", source_name="train", target_name=None, replace=False, branch=None, user=None),
    )

    dml.dag.checkout.assert_called_once_with(
        "origin/main",
        "train",
        branch=None,
        target_name=None,
        replace=False,
        user=None,
    )
    assert result == "commit:2"


def test_execute_dag_delete_delegates_to_dml_namespace():
    dml = Mock()
    dml.dag.delete.return_value = "commit:3"

    result = execute_dag_delete(dml, Namespace(name="train", branch="main", user="alice"))

    dml.dag.delete.assert_called_once_with("train", branch="main", user="alice")
    assert result == "commit:3"
