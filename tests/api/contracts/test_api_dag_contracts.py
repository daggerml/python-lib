from __future__ import annotations

from unittest.mock import call, patch

import pytest

import daggerml.api as api
from daggerml._core import DmlRepoError, Error


def test_api_dag_001__require_index_ref_rejects_committed_dag(fake_dml):
    dag = api.Dag(dml=fake_dml)

    with pytest.raises(DmlRepoError, match="No active index"):
        dag._require_index_ref()


def test_api_dag_002__make_node_classifies_value_shapes(fake_dml, refs):
    dag = api.Dag(dml=fake_dml, token=refs.index)

    assert isinstance(api._make_node(dag, refs.scalar), api.ScalarNode)
    assert isinstance(api._make_node(dag, refs.list), api.ListNode)
    assert isinstance(api._make_node(dag, refs.dict), api.DictNode)
    assert isinstance(api._make_node(dag, refs.runnable), api.RunnableNode)

    dict_node = api._make_node(dag, refs.dict)
    assert dict_node.type == "dict"
    assert dict_node.keys() == ["a", "b"]


def test_api_dag_003__put_applies_codecs_and_wraps_ref(dag, fake_dml, refs):
    node = dag.put(42, name="answer")

    fake_dml.runtime.put_literal.assert_called_with(refs.index, 42, name="answer")
    assert isinstance(node, api.ScalarNode)
    assert node.ref == refs.scalar


def test_api_dag_004__named_access_uses_partial_dag_description_for_uncommitted_dag(dag, fake_dml, refs):
    node = dag["a"]

    fake_dml.runtime.describe.assert_called_once_with(refs.index)
    fake_dml.dag.describe.assert_called_once_with(refs.dag)
    fake_dml.runtime.get_node.assert_not_called()
    assert isinstance(node, api.ScalarNode)


def test_api_dag_005__named_access_uses_description_for_committed_dag(fake_dml, refs):
    dag = api.Dag(dml=fake_dml, ref=refs.dag)

    node = dag["a"]

    fake_dml.dag.describe.assert_called_with(refs.dag)
    assert node.ref == refs.scalar


def test_api_dag_006__missing_committed_name_raises(fake_dml, refs):
    dag = api.Dag(dml=fake_dml, ref=refs.dag)

    with pytest.raises(DmlRepoError, match="Node 'missing' not found in DAG"):
        dag["missing"]


def test_api_dag_007__set_named_node_with_raw_value_stages_value(dag, fake_dml, refs):
    dag.answer = 42

    fake_dml.runtime.put_literal.assert_called_with(refs.index, 42, name="answer")


def test_api_dag_008__set_named_node_with_node_or_ref_updates_name(dag, fake_dml, refs):
    node = api.Node(dag, refs.scalar)

    dag._set_named_node("node", node)
    dag._set_named_node("ref", refs.dict)

    fake_dml.runtime.set_node_name.assert_any_call(refs.index, "node", refs.scalar)
    fake_dml.runtime.set_node_name.assert_any_call(refs.index, "ref", refs.dict)


def test_api_dag_009__set_named_node_rejects_committed_dag(fake_dml, refs):
    dag = api.Dag(dml=fake_dml, ref=refs.dag)

    with pytest.raises(DmlRepoError, match="Cannot set node names on a committed DAG"):
        dag._set_named_node("answer", 42)


def test_api_dag_010__keys_values_len_and_iter_use_described_names(dag, refs):
    assert dag.keys() == ["a", "z"]
    assert len(dag) == 2
    assert list(dag) == ["a", "z"]
    assert [node.ref for node in dag.values()] == [refs.dict, refs.scalar]


def test_api_dag_011__argv_and_result_require_refs(dag, fake_dml, refs):
    assert dag.argv.ref == refs.argv


    committed = api.Dag(dml=fake_dml, ref=refs.dag)
    assert committed.result.ref == refs.result

    fake_dml.dag.describe.return_value = {"names": {}, "argv": None, "result": None}
    with pytest.raises(DmlRepoError, match="dag has no argv"):
        value = dag.argv
        assert value is None
    with pytest.raises(DmlRepoError, match="Cannot access result of an uncommitted DAG"):
        value = dag.result
        assert value is None
    with pytest.raises(DmlRepoError, match="dag has not been committed yet"):
        value = committed.result
        assert value is None


def test_api_dag_012__require_imports_result_and_named_nodes(dag, fake_dml, refs):
    fake_dml.dag.describe.return_value = {"names": {"data": refs.dict}, "result": refs.result}

    result = dag.require("demo", name="required-result")
    named = dag.require("demo", "data", name="required-data")

    fake_dml.show.assert_any_call(revision=refs.commit)
    fake_dml.runtime.put_import.assert_any_call(refs.index, refs.dag, refs.result, name="required-result")
    fake_dml.runtime.put_import.assert_any_call(refs.index, refs.dag, refs.dict, name="required-data")
    assert result.ref == refs.imported
    assert named.ref == refs.imported


def test_api_dag_013__require_reports_missing_dag_or_node(dag, fake_dml, refs):
    with pytest.raises(DmlRepoError, match="DAG not found: missing"):
        dag.require("missing")

    fake_dml.dag.describe.return_value = {"names": {}, "result": None}
    with pytest.raises(DmlRepoError, match="Node 'data' not found in DAG 'demo'"):
        dag.require("demo", "data")

    with pytest.raises(DmlRepoError, match="Node 'None' not found in DAG 'demo'"):
        dag.require("demo")


def test_api_dag_014__call_builtin_stages_runnable_and_non_ref_args(dag, fake_dml, refs):
    result = dag._call_builtin("daggerml:get", refs.dict, "a", name="value")

    assert result == refs.result
    runnable_value = fake_dml.runtime.put_literal.call_args_list[0].args[1]
    assert isinstance(runnable_value, api.Runnable)
    assert runnable_value.target == api.Uri("daggerml:get")
    assert fake_dml.runtime.start_fn.call_args.args == (refs.index, [refs.scalar, refs.dict, refs.scalar])
    assert fake_dml.runtime.start_fn.call_args.kwargs == {"name": "value"}


def test_api_dag_015__call_builtin_raises_when_execution_fails(dag, fake_dml):
    fake_dml.runtime.start_fn.return_value = None

    with pytest.raises(DmlRepoError, match="Function execution failed"):
        dag._call_builtin("daggerml:missing")


def test_api_dag_016__call_retries_until_result_and_times_out(dag, fake_dml, refs):
    fake_dml.runtime.start_fn.side_effect = [None, refs.result]

    with patch.object(api.time, "sleep") as sleep:
        node = dag.call("fn", 1, name="out", sleep=lambda: 0, timeout=10)

    assert node.ref == refs.result
    sleep.assert_called_once_with(0)

    fake_dml.runtime.start_fn.side_effect = None
    fake_dml.runtime.start_fn.return_value = None
    with pytest.raises(TimeoutError, match="invoking function"):
        dag.call("fn", sleep=lambda: 0, timeout=1)


def test_api_dag_017__commit_handles_raw_node_and_error_values(dag, fake_dml, refs):
    dag.commit(42)
    fake_dml.runtime.commit.assert_called_with(refs.index, refs.scalar, message="msg", name="demo")
    assert dag.ref == refs.dag

    fake_dml.runtime.commit.reset_mock()
    dag = api.Dag(dml=fake_dml, token=refs.index, name="demo", message="msg")
    node = api.Node(dag, refs.dict)
    dag.commit(node)
    fake_dml.runtime.commit.assert_called_with(refs.index, refs.dict, message="msg", name="demo")

    fake_dml.runtime.commit.reset_mock()
    dag = api.Dag(dml=fake_dml, token=refs.index, name="demo", message="msg")
    err = Error("boom", origin="test", type="RuntimeError")
    dag.commit(err)
    fake_dml.runtime.commit.assert_called_with(refs.index, err, message="msg", name="demo")


def test_api_dag_018__context_manager_commits_exceptions(dag):
    with patch.object(dag, "commit") as commit:
        assert dag.__enter__() is dag
        dag.__exit__(RuntimeError, RuntimeError("boom"), None)

    committed = commit.call_args.args[0]
    assert isinstance(committed, Error)
    assert committed.message == "boom"

    err = Error("stored", origin="test", type="RuntimeError")
    with patch.object(dag, "commit") as commit:
        dag.__exit__(Error, err, None)
    commit.assert_called_once_with(err)


def test_api_dag_019__context_manager_rejects_committed_dag(fake_dml, refs):
    with pytest.raises(AssertionError):
        api.Dag(dml=fake_dml, ref=refs.dag).__enter__()


def test_api_dag_020__private_missing_attribute_raises_attribute_error(dag):
    with pytest.raises(AttributeError, match="_missing"):
        value = dag._missing
        assert value is None


def test_api_dag_021__repr_uses_ref_token_or_na(fake_dml, refs):
    assert repr(api.Dag(dml=fake_dml, ref=refs.dag)) == f"Dag({refs.dag.to})"
    assert repr(api.Dag(dml=fake_dml, token=refs.index)) == f"Dag({refs.index.to})"
    assert repr(api.Dag(dml=fake_dml)) == "Dag(NA)"


def test_api_dag_022__freeze_and_unfreeze_replace_only_token_and_return_self(dag, fake_dml, refs):
    frozen = api.Ref("frozenindex:frozen")
    fake_dml.runtime.freeze.return_value = frozen
    fake_dml.runtime.unfreeze.return_value = refs.index
    original = (dag.dml, dag.ref, dag.name, dag.message)

    assert dag.freeze("checkpoint") is dag
    fake_dml.runtime.freeze.assert_called_once_with(refs.index, message="dag: demo\ncheckpoint")
    assert dag.token == frozen
    assert (dag.dml, dag.ref, dag.name, dag.message) == original

    assert dag.unfreeze() is dag
    fake_dml.runtime.unfreeze.assert_called_once_with(frozen)
    assert dag.token == refs.index
    assert (dag.dml, dag.ref, dag.name, dag.message) == original

    fake_dml.runtime.freeze.reset_mock()
    assert dag.freeze() is dag
    fake_dml.runtime.freeze.assert_called_once_with(refs.index, message="dag: demo")


def test_api_dag_023__frozen_index_reads_use_partial_dag_projections(dag, fake_dml, refs):
    frozen = api.Ref("frozenindex:frozen")
    dag.token = frozen
    fake_dml.runtime.describe.return_value = {"dag": refs.dag2}

    assert dag["a"].ref == refs.scalar
    assert dag.keys() == ["a", "z"]
    assert [node.ref for node in dag.values()] == [refs.dict, refs.scalar]
    assert dag.argv.ref == refs.argv

    assert fake_dml.runtime.describe.call_args_list == [call(frozen)] * 4
    assert fake_dml.dag.describe.call_args_list == [call(refs.dag2)] * 4
    fake_dml.runtime.get_node.assert_not_called()


def test_api_dag_024__frozen_index_remains_uncommitted_and_does_not_auto_unfreeze(dag, fake_dml, refs):
    frozen = api.Ref("frozenindex:frozen")
    dag.token = frozen

    with pytest.raises(DmlRepoError, match="Cannot access result of an uncommitted DAG"):
        value = dag.result
        assert value is None

    fake_dml.runtime.put_literal.side_effect = DmlRepoError("Cannot mutate a frozen index")
    with pytest.raises(DmlRepoError, match="Cannot mutate a frozen index"):
        dag.put(42)
    fake_dml.runtime.put_literal.assert_called_with(frozen, 42, name=None)
    fake_dml.runtime.unfreeze.assert_not_called()
