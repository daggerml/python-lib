from argparse import Namespace
from unittest.mock import Mock, patch

import daggerml._cli.dag as dag_cli
from daggerml._cli.dag import execute_dag_checkout


@patch("daggerml._cli.dag.parse_ref")
def test_execute_dag_checkout_delegates_to_dmlops(mock_parse_ref):
    ops = Mock(path="/repo/from-flag", _db=Mock())
    ops.checkout_dag_from_revision.return_value = "commit:2"
    mock_parse_ref.return_value = "head:feature"

    result = execute_dag_checkout(
        ops,
        Namespace(
            revision="origin/main",
            source_name="train",
            target_name=None,
            replace=False,
            head=None,
            branch=None,
            user=None,
        ),
    )

    ops.checkout_dag_from_revision.assert_called_once_with(
        "origin/main",
        "train",
        target_name=None,
        replace=False,
        head=None,
        branch=None,
        user=None,
    )
    mock_parse_ref.assert_not_called()
    assert result == "commit:2"


@patch("daggerml._cli.dag.parse_ref")
def test_execute_dag_checkout_parses_explicit_head(mock_parse_ref):
    ops = Mock(path="/repo/from-flag", _db=Mock())
    mock_parse_ref.return_value = "head:feature"

    execute_dag_checkout(
        ops,
        Namespace(
            revision="origin/main",
            source_name="train",
            target_name=None,
            replace=False,
            head="head:feature",
            branch=None,
            user="explicit",
        ),
    )

    ops.checkout_dag_from_revision.assert_called_once_with(
        "origin/main",
        "train",
        target_name=None,
        replace=False,
        head="head:feature",
        branch=None,
        user="explicit",
    )


def test_dag_cli_module_avoids_internal_orchestration_imports():
    assert not hasattr(dag_cli, "CommitOps")
    assert not hasattr(dag_cli, "DmlProjectConfig")
    assert not hasattr(dag_cli, "DmlConfig")
